"""
NOAA WaveWatch III(WW3) 파랑 예보 데이터 다운로드 모듈
======================================================

데이터 소스: PacIOOS ERDDAP — ww3_global
  URL: https://pae-paha.pacioos.hawaii.edu/erddap/griddap/ww3_global
  제공 기관: Pacific Islands Ocean Observing System (PacIOOS)
  모델: NOAA WaveWatch III 전지구 파랑 예보 모델

ECMWF와의 역할 분담:
  - ECMWF 예보 (env_ecmwf_forecast): 바람(u10, v10) 담당
    → wave 컬럼(swh/mwd/mwp/shts/mdts/mpts/shww/mdww/mpww)은 현재 NULL
  - NOAA WW3 예보 (env_noaa_forecast): 파랑 9개 변수 전담
    → 너울+풍파 분리 정보까지 완전 제공

NOAA WW3 변수명 → DB 컬럼명 매핑:
  Thgt → swh   (복합 유의파고, m)     : 풍파 + 너울 합산
  Tdir → mwd   (평균 파랑 방향, °)   : 파랑 진행 방향 (0=북)
  Tper → mwp   (평균 파랑 주기, s)   : 파랑 한 주기의 길이
  shgt → shts  (너울 유의파고, m)    : swell significant height
  sdir → mdts  (너울 방향, °)
  sper → mpts  (너울 주기, s)
  whgt → shww  (풍파 유의파고, m)    : wind wave significant height
  wdir → mdww  (풍파 방향, °)
  wper → mpww  (풍파 주기, s)

데이터 특성:
  - 해상도: 0.5° × 0.5° (ECMWF 0.25°보다 낮음)
  - 시간 간격: 1시간 (PacIOOS ww3_global 기준)
  - 좌표: 경도 0~360 → 다운로드 시 -180~180으로 변환
  - 예보 기간: 약 7일 (PacIOOS 기준)

저장 경로: data/noaa/forecast/YYYY/MM/noaa_fc_wave_YYYYMMDD.nc
  - YYYYMMDD: 다운로드 실행일 (issued_at)
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import xarray as xr         # OPeNDAP / NetCDF 처리
import numpy as np
from loguru import logger


# ─────────────────────────────────────────────────────
# PacIOOS WW3 OPeNDAP URL
# ─────────────────────────────────────────────────────

# PacIOOS ERDDAP griddap — ww3_global Best Time Series
# 특징:
#   - 2017년부터 현재(+7일 예보)까지 연속 데이터 제공
#   - ERDDAP griddap은 OPeNDAP 프로토콜 호환 → xarray로 직접 접근 가능
#   - 무료, 인증 불필요
NOAA_WW3_URL = (
    "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/ww3_global"
)

# ─────────────────────────────────────────────────────
# NOAA WW3 변수명 → DB 컬럼명 매핑
# ─────────────────────────────────────────────────────

# NOAA WW3 원래 변수명은 약어(Thgt, Tdir 등)을 사용
# 우리 DB는 ECMWF 컬럼명(swh, mwd 등)으로 통일
# → 다운로드 직후 rename하여 일관성 유지
VARIABLE_RENAME = {
    "Thgt": "swh",    # 복합 유의파고 (Total Height)
    "Tdir": "mwd",    # 복합 파랑 방향 (Total Direction)
    "Tper": "mwp",    # 복합 파랑 주기 (Total Period)
    "shgt": "shts",   # 너울 유의파고 (Swell Height)
    "sdir": "mdts",   # 너울 방향 (Swell Direction)
    "sper": "mpts",   # 너울 주기 (Swell Period)
    "whgt": "shww",   # 풍파 유의파고 (Wind-Wave Height)
    "wdir": "mdww",   # 풍파 방향 (Wind-Wave Direction)
    "wper": "mpww",   # 풍파 주기 (Wind-Wave Period)
}

# 원래 NOAA 변수명 목록 (수집 대상)
NOAA_VARIABLES = list(VARIABLE_RENAME.keys())


class NOAAForecastDownloader:
    """
    NOAA WaveWatch III 파랑 예보 데이터 다운로더

    PacIOOS ERDDAP OPeNDAP URL에서 오늘부터 N일치 파랑 예보를
    한 파일로 다운로드하여 NetCDF로 저장.

    HYCOMForecastDownloader와 동일한 구조 (OPeNDAP 스트리밍 방식).

    사용 예:
        downloader = NOAAForecastDownloader(
            output_dir=Path("data/noaa/forecast"),
            forecast_days=5,
        )
        paths = downloader.run()
    """

    def __init__(self, output_dir: Path, forecast_days: int = 5):
        """
        Parameters
        ----------
        output_dir    : Path  →  NetCDF 저장 최상위 경로
        forecast_days : int   →  예보 기간 (일, PacIOOS WW3는 최대 약 7일)
        """
        self.output_dir    = output_dir
        # PacIOOS WW3 예보 가능 기간: 약 7일 제한
        self.forecast_days = min(forecast_days, 7)

        # 저장 폴더가 없으면 자동 생성
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"NOAAForecastDownloader 준비 완료 | "
            f"예보 기간: {self.forecast_days}일 | "
            f"저장 경로: {self.output_dir}"
        )

    def _get_output_path(self, issued_date: datetime) -> Path:
        """
        저장 파일 경로 생성

        경로 예: data/noaa/forecast/2026/03/noaa_fc_wave_20260316.nc

        Parameters
        ----------
        issued_date : datetime  →  예보 실행일 (오늘 날짜)
        """
        # 연/월 폴더 생성 (없으면 자동 생성)
        folder = self.output_dir / f"{issued_date.year:04d}" / f"{issued_date.month:02d}"
        folder.mkdir(parents=True, exist_ok=True)

        # 파일명: noaa_fc_wave_YYYYMMDD.nc
        filename = f"noaa_fc_wave_{issued_date.strftime('%Y%m%d')}.nc"
        return folder / filename

    def run(self) -> list[Path]:
        """
        NOAA WW3 파랑 예보 데이터 다운로드

        오늘 00:00 UTC ~ 오늘 + forecast_days 범위의 예보를 한 파일로 저장.
        항상 최신 예보 기준으로 1회 실행.

        Returns
        -------
        list[Path]  →  성공 시 파일 경로 1개짜리 리스트, 실패 시 빈 리스트
        """
        logger.info("=" * 50)
        logger.info("NOAA WW3 파랑 예보 다운로드 시작")
        logger.info("=" * 50)

        # ── UTC 기준 오늘 날짜를 issued_at으로 사용 ──
        # 예: 2026-03-16 00:00:00 UTC
        now_utc   = datetime.now(timezone.utc).replace(tzinfo=None)
        issued_at = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

        # 예보 시간 범위: 오늘 00:00 ~ 오늘 + forecast_days 23:59
        forecast_start = issued_at
        forecast_end   = issued_at + timedelta(days=self.forecast_days)

        # 출력 파일 경로 결정
        output_path = self._get_output_path(issued_at)

        # ── 이미 파일이 있으면 건너뜀 ──
        if output_path.exists() and output_path.stat().st_size > 0:
            size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"[건너뜀] {output_path.name} ({size_mb:.1f} MB)")
            return [output_path]

        logger.info(
            f"[다운로드] NOAA WW3 파랑 예보 | "
            f"{forecast_start.strftime('%Y-%m-%d')} ~ "
            f"{forecast_end.strftime('%Y-%m-%d')} | "
            f"9개 변수"
        )

        try:
            # ── 1단계: OPeNDAP URL로 원격 데이터셋 열기 ──
            # PacIOOS ERDDAP griddap은 OPeNDAP 프로토콜 지원
            # → HYCOM과 동일하게 xr.open_dataset()으로 접근 가능
            # 이 단계에서는 실제 데이터를 다운로드하지 않고 메타정보만 가져옴
            logger.debug("  PacIOOS WW3 URL 연결 중...")
            # decode_timedelta=False:
            #   PacIOOS WW3의 sper(너울 주기), wper(풍파 주기) 변수는
            #   units="seconds"를 가져 xarray가 timedelta64로 잘못 해석함.
            #   이를 방지하여 수치(초 단위 float)로 그대로 읽도록 설정.
            ds = xr.open_dataset(NOAA_WW3_URL, decode_timedelta=False)

            # ── 2단계: 예보 시간 범위 선택 ──
            # PacIOOS WW3는 "time" 좌표 사용 (ERDDAP 표준)
            day_start = forecast_start.strftime("%Y-%m-%dT00:00:00")
            day_end   = forecast_end.strftime("%Y-%m-%dT23:59:59")

            # 9개 변수를 시간 범위로 선택 (공간은 전체)
            ds_fc = ds[NOAA_VARIABLES].sel(
                time=slice(day_start, day_end)
            )

            # 선택된 시간 스텝 수 확인
            time_count = len(ds_fc.time)
            logger.info(f"  예보 시간 스텝: {time_count}개")

            if time_count == 0:
                logger.warning(
                    f"  예보 데이터가 없습니다. "
                    f"예보 시작: {day_start} / 끝: {day_end}\n"
                    f"  PacIOOS WW3는 최신 약 7일 예보만 보유합니다."
                )
                ds.close()
                return []

            # ── 3단계: 경도 변환 0~360 → -180~180 ──
            # PacIOOS WW3 경도: 0~359.5 (0.5° 간격)
            # ECMWF/HYCOM은 -180~180 기준 → 일관성 유지를 위해 변환
            #
            # 좌표 이름 확인: ERDDAP은 "longitude"를 사용할 수 있음
            lon_coord = None
            for cname in ("longitude", "lon"):
                if cname in ds_fc.coords:
                    lon_coord = cname
                    break

            if lon_coord and float(ds_fc[lon_coord].values.max()) > 180:
                # (lon + 180) % 360 - 180: 270° → -90°, 130° → 130°
                ds_fc = ds_fc.assign_coords(
                    **{lon_coord: ((ds_fc[lon_coord] + 180) % 360 - 180)}
                ).sortby(lon_coord)   # 변환 후 경도 오름차순으로 재정렬
                logger.debug("  경도 변환 완료: 0~360 → -180~180")

            # ── 4단계: 변수명 변환 NOAA → DB 컬럼명 ──
            # NOAA WW3 변수명(Thgt, Tdir 등)을 DB 컬럼명(swh, mwd 등)으로 변경
            # ECMWF와 동일한 컬럼명을 사용하여 LLM 쿼리 통일성 확보
            ds_fc = ds_fc.rename(VARIABLE_RENAME)
            logger.debug(
                f"  변수명 변환 완료: "
                f"{list(VARIABLE_RENAME.keys())} → {list(VARIABLE_RENAME.values())}"
            )

            # ── 5단계: 실제 데이터 메모리에 로드 ──
            # 지금까지는 "무엇을 가져올지" 정의만 했음
            # .load() 호출 시 실제 네트워크 전송 발생
            logger.debug("  예보 데이터 전송 중... (수 초 ~ 수분 소요)")
            ds_fc = ds_fc.load()

            # ── 5.5단계: timedelta64 변수를 float(초 단위)로 변환 ──
            # PacIOOS WW3의 sper(→mpts), wper(→mpww)는 units="seconds"를 가져
            # decode_timedelta=False로 막았어도 일부 xarray 버전에서 여전히
            # timedelta64로 해석될 수 있음. NetCDF 저장 전 float으로 확실히 변환.
            for var in list(ds_fc.data_vars):
                if ds_fc[var].dtype.kind == "m":   # 'm' = timedelta dtype
                    # timedelta64[ns] → float32 (나노초 → 초 단위 변환)
                    float_vals = (
                        ds_fc[var] / np.timedelta64(1, "s")
                    ).astype("float32")

                    # 기존 attrs 복사 후 units를 xarray가 timedelta로
                    # 오인하지 않는 문자열로 변경
                    # ("seconds" / "s" 모두 xarray timedelta 감지 대상이므로
                    #  "wave_seconds"로 변경 → NetCDF 읽기 시 재해석 방지)
                    old_attrs = dict(ds_fc[var].attrs)
                    old_attrs["units"]         = "wave_seconds"
                    old_attrs["original_units"] = old_attrs.pop("units", "seconds")

                    # DataArray 교체: 좌표(dims)는 유지하고 값·속성만 교체
                    ds_fc[var] = xr.DataArray(
                        float_vals.values,
                        coords=float_vals.coords,
                        dims=float_vals.dims,
                        attrs=old_attrs,
                    )
                    logger.debug(
                        f"  timedelta 변환 완료: {var} → float32 "
                        f"(units: wave_seconds)"
                    )

            # ── 6단계: issued_at 및 메타데이터 저장 ──
            # loader.py가 issued_at 속성을 읽어 DB의 issued_at 컬럼에 사용
            ds_fc.attrs["issued_at"]    = issued_at.isoformat()
            ds_fc.attrs["forecast_days"] = self.forecast_days
            ds_fc.attrs["source"]        = "NOAA WaveWatch III (PacIOOS)"
            ds_fc.attrs["wave_params"]   = list(VARIABLE_RENAME.values())  # swh, mwd, ...

            # ── 7단계: NetCDF 파일로 저장 ──
            ds_fc.to_netcdf(str(output_path))
            ds.close()   # 원격 OPeNDAP 연결 닫기

            size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.success(
                f"[완료] {output_path.name} | "
                f"{size_mb:.1f} MB | "
                f"시간 스텝 {time_count}개 | "
                f"변수 {len(VARIABLE_RENAME)}개"
            )
            return [output_path]

        except Exception as e:
            logger.error(f"[실패] NOAA WW3 예보 다운로드: {e}")
            # 실패로 생긴 불완전한 파일 삭제 (다음 실행 시 재시도 가능하도록)
            if output_path.exists():
                output_path.unlink()
            # 원격 연결 닫기
            try:
                ds.close()
            except Exception:
                pass
            return []
