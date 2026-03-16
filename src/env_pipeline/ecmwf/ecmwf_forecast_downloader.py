"""
ECMWF HRES 예보 데이터 다운로드 모듈
======================================

ECMWF HRES(High-Resolution Forecast)란?
  - 유럽중기예보센터(ECMWF)의 고해상도 결정론적 예보 모델
  - 세계 최고 수준의 기상 예보 정확도
  - 바람(u10, v10) + 파랑(swh, mwd, mwp 등) ERA5와 동일 변수 제공

접근 방식: ECMWF Open Data (ecmwf-opendata 라이브러리)
  - CDS API(재분석용)와 완전히 별개의 라이브러리
  - 인증 불필요 (무료 공개 데이터)
  - 다운로드 형식: GRIB2 → cfgrib으로 xarray 변환 → NetCDF 저장

예보 기간: 발행일 기준 최대 10일 (0~240시간)
시간 간격: 6시간 (0, 6, 12, ..., 240시 → 41 스텝)
공간 해상도: 0.25° (전세계)
경도 변환: 0~360 → -180~180 (ERA5 재분석과 좌표계 통일)

저장 경로: data/ecmwf/forecast/YYYY/MM/ecmwf_fc_{wind|wave}_YYYYMMDD.nc
  - YYYYMMDD: 예보 발행일 (issued_at)
  - NetCDF 전역 속성에 issued_at 저장 (DB 적재 시 사용)
"""

import tempfile           # 임시 GRIB2 파일 저장용 (다운로드 후 삭제)
from datetime import datetime, timezone
from pathlib import Path

import numpy as np        # 수치 연산
import pandas as pd       # 시간 처리
import xarray as xr       # NetCDF/GRIB2 데이터 처리
from loguru import logger


# ─────────────────────────────────────────────────────
# 예보 변수 정의
# ─────────────────────────────────────────────────────

# ECMWF Open Data에서 사용하는 GRIB2 단축 이름 (short name)
# ERA5와 동일한 물리 변수지만 API 접근 방법이 다름

# 바람 예보 변수 (단축명 → DB 컬럼명)
# ecmwf-opendata: "10u", "10v" / cfgrib 변환 후: "u10", "v10"
WIND_PARAMS = ["10u", "10v"]   # GRIB2 short name 기준

# 파랑 예보 변수 (단축명)
# 전체 9개 시도 → 실패 시 기본 3개로 fallback
WAVE_PARAMS_FULL = [
    "swh",   # 복합 유의파고 (m)
    "mwd",   # 평균 파랑 방향 (°)
    "mwp",   # 평균 파랑 주기 (s)
    "shts",  # 너울 유의파고 (m)   ← Open Data 미제공 시 NaN
    "mdts",  # 너울 방향 (°)       ← Open Data 미제공 시 NaN
    "mpts",  # 너울 주기 (s)       ← Open Data 미제공 시 NaN
    "shww",  # 풍파 유의파고 (m)
    "mdww",  # 풍파 방향 (°)
    "mpww",  # 풍파 주기 (s)
]
WAVE_PARAMS_BASIC = ["swh", "mwd", "mwp"]  # 반드시 제공되는 기본 3개 (fallback)


def _build_forecast_steps(step_hours: int) -> list[int]:
    """
    예보 스텝 목록 생성 (시간 단위)

    ECMWF HRES는 0~240시간(10일)을 제공하며,
    step_hours 간격으로 스텝 목록을 생성한다.

    Parameters
    ----------
    step_hours : int  →  스텝 간격 (시간 단위, 보통 6)

    Returns
    -------
    list[int]  →  [0, 6, 12, ..., 240] 형태의 정수 리스트
    """
    # range(시작, 끝+1, 간격) → 0부터 240까지 step_hours 간격
    return list(range(0, 241, step_hours))


def _grib2_to_dataset(grib_path: Path, params: list[str]) -> xr.Dataset:
    """
    GRIB2 파일을 xarray Dataset으로 변환 (변수별 개별 읽기 후 병합)

    cfgrib는 동일 파일에 여러 변수가 있을 때 각각 따로 읽는 것이 안정적.
    각 변수를 filter_by_keys로 선택 후 merge로 합친다.

    Parameters
    ----------
    grib_path : Path         →  GRIB2 파일 경로
    params    : list[str]    →  읽을 GRIB2 short name 목록

    Returns
    -------
    xr.Dataset  →  변수들이 합쳐진 Dataset
    """
    datasets = []   # 변수별 Dataset을 담을 리스트

    for param in params:
        try:
            # filter_by_keys: 특정 short name의 변수만 선택
            # indexpath='': 인덱스 파일(.idx)을 현재 폴더에 만들지 않음
            ds_var = xr.open_dataset(
                str(grib_path),
                engine="cfgrib",
                filter_by_keys={"shortName": param},
                backend_kwargs={"indexpath": ""},
            )
            datasets.append(ds_var)
            logger.debug(f"  변수 읽기 성공: {param}")

        except Exception as e:
            # 해당 변수가 파일에 없거나 읽기 실패 → 건너뜀 (NaN으로 처리 예정)
            logger.warning(f"  변수 읽기 실패 [{param}]: {e}")

    if not datasets:
        # 하나도 읽지 못한 경우 → 오류
        raise ValueError(f"읽을 수 있는 변수가 없습니다: {grib_path.name}")

    # 변수별 Dataset을 하나로 병합
    # compat='override': 좌표 충돌 시 첫 번째 Dataset 기준 사용
    return xr.merge(datasets, compat="override")


def _restructure_forecast_dataset(ds: xr.Dataset) -> tuple[xr.Dataset, pd.Timestamp]:
    """
    cfgrib로 읽은 예보 Dataset을 적재 가능한 형태로 변환

    cfgrib 예보 Dataset 구조:
      - time (scalar): 예보 발행 시각 (reference time = issued_at)
      - step (N,): 예보 스텝 (timedelta: 0h, 6h, ..., 240h)
      - valid_time (N,): 실제 예보 시각 = time + step
      - latitude (M,), longitude (K,)
      - 변수들: shape (N, M, K)

    변환 목표:
      - step 차원 → valid_time 차원으로 변경
      - 경도 0~360 → -180~180 변환
      - issued_at 추출 및 반환

    Parameters
    ----------
    ds : xr.Dataset  →  cfgrib로 읽은 원본 Dataset

    Returns
    -------
    tuple[xr.Dataset, pd.Timestamp]  →  (변환된 Dataset, issued_at)
    """
    # ── 1단계: issued_at (예보 발행 시각) 추출 ──
    # cfgrib에서 'time' 좌표는 scalar (예보 run time = 발행 시각)
    if "time" in ds.coords:
        issued_at = pd.Timestamp(ds["time"].values)
    else:
        # 'time' 좌표가 없는 경우 현재 UTC 시간으로 대체
        issued_at = pd.Timestamp.now(tz="UTC").replace(tzinfo=None)
        logger.warning("  issued_at을 파일에서 읽지 못함 → 현재 시각으로 대체")

    logger.debug(f"  예보 발행 시각 (issued_at): {issued_at}")

    # ── 2단계: step 차원 → valid_time 차원으로 변경 ──
    # step (timedelta) 대신 valid_time (실제 날짜시각)을 주 차원으로 사용
    if "step" in ds.dims:
        # valid_time을 좌표로 설정
        ds = ds.assign_coords(valid_time=ds["valid_time"])
        # step 차원 이름을 valid_time으로 교체
        ds = ds.swap_dims({"step": "valid_time"})
        # 불필요한 좌표 제거 (scalar time, timedelta step)
        ds = ds.drop_vars(["step", "time"], errors="ignore")

    # ── 3단계: 경도 0~360 → -180~180 변환 ──
    # ECMWF GRIB2는 0~360 경도 사용 → ERA5 재분석(-180~180)과 통일
    lon_coord = None
    if "longitude" in ds.coords:
        lon_coord = "longitude"
    elif "lon" in ds.coords:
        lon_coord = "lon"

    if lon_coord and float(ds[lon_coord].values.max()) > 180:
        ds = ds.assign_coords(
            **{lon_coord: ((ds[lon_coord] + 180) % 360 - 180)}
        ).sortby(lon_coord)
        logger.debug("  경도 변환 완료: 0~360 → -180~180")

    return ds, issued_at


class ECMWFForecastDownloader:
    """
    ECMWF HRES 예보 데이터 다운로더

    사용 예:
        downloader = ECMWFForecastDownloader(
            output_dir=Path("data/ecmwf/forecast"),
            forecast_days=10,
            step_hours=6,
        )
        paths = downloader.run()
    """

    def __init__(
        self,
        output_dir: Path,
        forecast_days: int = 10,
        step_hours: int = 6,
    ):
        """
        Parameters
        ----------
        output_dir    : Path  →  NetCDF 파일 저장 최상위 경로
        forecast_days : int   →  예보 기간 (일, 최대 10)
        step_hours    : int   →  예보 스텝 간격 (시간, 기본 6)
        """
        self.output_dir   = output_dir
        self.forecast_days = min(forecast_days, 10)   # 최대 10일
        self.step_hours   = step_hours
        self.max_step_h   = self.forecast_days * 24   # 최대 시간 스텝

        # 저장 폴더 생성
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"ECMWFForecastDownloader 준비 완료 | "
            f"예보 기간: {self.forecast_days}일 | "
            f"스텝: {self.step_hours}시간 간격 | "
            f"저장 경로: {self.output_dir}"
        )

    def _get_output_path(self, data_type: str, issued_date: datetime) -> Path:
        """
        저장 파일 경로 생성

        경로 예: data/ecmwf/forecast/2026/03/ecmwf_fc_wind_20260315.nc
                                              ecmwf_fc_wave_20260315.nc

        Parameters
        ----------
        data_type   : str       →  "wind" 또는 "wave"
        issued_date : datetime  →  예보 발행일
        """
        # 연/월 폴더 생성
        folder = self.output_dir / f"{issued_date.year:04d}" / f"{issued_date.month:02d}"
        folder.mkdir(parents=True, exist_ok=True)

        # 파일명: 발행일 기준
        filename = f"ecmwf_fc_{data_type}_{issued_date.strftime('%Y%m%d')}.nc"
        return folder / filename

    def _download_wind(self, steps: list[int], tmp_dir: str) -> tuple[Path | None, pd.Timestamp | None]:
        """
        바람 예보 GRIB2 다운로드 및 NetCDF 변환

        Parameters
        ----------
        steps   : list[int]  →  예보 스텝 목록 (시간 단위)
        tmp_dir : str        →  임시 GRIB2 파일 저장 폴더

        Returns
        -------
        tuple[Path | None, pd.Timestamp | None]  →  (저장 경로, issued_at)
        """
        # 임시 GRIB2 파일 경로
        grib_path = Path(tmp_dir) / "ecmwf_fc_wind_temp.grib2"

        try:
            # ecmwf-opendata 클라이언트 (여기서 import: 설치 여부 확인 목적)
            from ecmwf.opendata import Client  # ECMWF Open Data 라이브러리

            client = Client("ecmwf")  # ECMWF 공식 서버 사용

            logger.debug(f"  바람 GRIB2 다운로드 중... (스텝 수: {len(steps)})")

            # GRIB2 다운로드
            # date, time 미지정 시 최신 발행 예보 자동 선택
            client.retrieve(
                type="fc",                    # fc = forecast (예보)
                param=WIND_PARAMS,            # ["10u", "10v"]
                step=steps,                   # 예보 스텝 목록 (시간)
                target=str(grib_path),        # 저장할 파일 경로
            )

            logger.debug(f"  GRIB2 다운로드 완료: {grib_path.stat().st_size / 1024 / 1024:.1f} MB")

            # ── GRIB2 → xarray Dataset 변환 ──
            ds = _grib2_to_dataset(grib_path, WIND_PARAMS)
            ds, issued_at = _restructure_forecast_dataset(ds)

            # ── issued_at 기반으로 최종 저장 경로 결정 ──
            output_path = self._get_output_path("wind", issued_at.to_pydatetime())

            # 이미 파일이 있으면 건너뜀
            if output_path.exists() and output_path.stat().st_size > 0:
                size_mb = output_path.stat().st_size / (1024 * 1024)
                logger.info(f"[건너뜀] {output_path.name} ({size_mb:.1f} MB)")
                return output_path, issued_at

            # ── issued_at을 NetCDF 전역 속성에 저장 ──
            # 로더(loader.py)가 이 값을 읽어 DB issued_at 컬럼에 사용
            ds.attrs["issued_at"] = issued_at.isoformat()
            ds.attrs["forecast_days"] = self.forecast_days
            ds.attrs["source"] = "ECMWF Open Data HRES"

            # ── NetCDF 파일로 저장 ──
            ds.to_netcdf(str(output_path))

            size_mb = output_path.stat().st_size / (1024 * 1024)
            time_count = len(ds["valid_time"]) if "valid_time" in ds.dims else "?"
            logger.success(
                f"[완료] {output_path.name} | "
                f"{size_mb:.1f} MB | "
                f"스텝 수: {time_count}"
            )
            return output_path, issued_at

        except Exception as e:
            logger.error(f"[실패] 바람 예보 다운로드: {e}")
            return None, None

        finally:
            # 임시 GRIB2 파일 삭제 (용량 절약)
            if grib_path.exists():
                grib_path.unlink()

    def _download_wave(
        self,
        steps: list[int],
        tmp_dir: str,
        issued_at: pd.Timestamp | None,
    ) -> Path | None:
        """
        파랑 예보 GRIB2 다운로드 및 NetCDF 변환

        Parameters
        ----------
        steps     : list[int]               →  예보 스텝 목록
        tmp_dir   : str                     →  임시 파일 폴더
        issued_at : pd.Timestamp | None     →  바람 다운로드에서 얻은 발행 시각

        Returns
        -------
        Path | None  →  저장 경로
        """
        grib_path = Path(tmp_dir) / "ecmwf_fc_wave_temp.grib2"

        try:
            from ecmwf.opendata import Client

            client = Client("ecmwf")

            # ── 파랑 변수 다운로드 시도 (전체 9개 → 실패 시 기본 3개로 재시도) ──
            wave_params_used = WAVE_PARAMS_FULL

            for attempt, params in enumerate([WAVE_PARAMS_FULL, WAVE_PARAMS_BASIC], start=1):
                try:
                    logger.debug(
                        f"  파랑 GRIB2 다운로드 중... "
                        f"(시도 {attempt}/2, 변수 수: {len(params)})"
                    )
                    client.retrieve(
                        type="fc",
                        stream="wave",    # 파랑 모델 스트림 (대기 모델 "oper"와 별개)
                        param=params,
                        step=steps,
                        target=str(grib_path),
                    )
                    wave_params_used = params
                    logger.debug(f"  GRIB2 다운로드 완료: {grib_path.stat().st_size / 1024 / 1024:.1f} MB")
                    break  # 성공 시 반복 종료

                except Exception as e:
                    if attempt == 1:
                        # 전체 변수 실패 → 기본 변수로 재시도
                        logger.warning(
                            f"  파랑 전체 변수 실패 → 기본 변수(swh, mwd, mwp)로 재시도: {e}"
                        )
                        if grib_path.exists():
                            grib_path.unlink()
                    else:
                        # 기본 변수도 실패 → 오류
                        raise

            # ── GRIB2 → xarray Dataset 변환 ──
            ds = _grib2_to_dataset(grib_path, wave_params_used)
            ds, ds_issued_at = _restructure_forecast_dataset(ds)

            # issued_at: 바람에서 이미 구했으면 재사용, 아니면 파랑에서 사용
            final_issued_at = issued_at if issued_at is not None else ds_issued_at

            # ── 저장 경로 결정 ──
            output_path = self._get_output_path("wave", final_issued_at.to_pydatetime())

            # 이미 파일 있으면 건너뜀
            if output_path.exists() and output_path.stat().st_size > 0:
                size_mb = output_path.stat().st_size / (1024 * 1024)
                logger.info(f"[건너뜀] {output_path.name} ({size_mb:.1f} MB)")
                return output_path

            # 받은 파랑 변수 목록 로그
            downloaded_vars = [v for v in wave_params_used if v in ds.data_vars or v in ds.coords]
            logger.info(f"  다운로드된 파랑 변수: {downloaded_vars}")
            if wave_params_used == WAVE_PARAMS_BASIC:
                logger.warning(
                    "  ⚠ 기본 변수만 다운로드됨 (너울 성분 없음). "
                    "ECMWF Open Data에서 shts/mdts/mpts 미제공일 수 있음."
                )

            # ── NetCDF 전역 속성 저장 ──
            ds.attrs["issued_at"] = final_issued_at.isoformat()
            ds.attrs["forecast_days"] = self.forecast_days
            ds.attrs["source"] = "ECMWF Open Data HRES"
            ds.attrs["wave_params"] = str(wave_params_used)

            # ── NetCDF 저장 ──
            ds.to_netcdf(str(output_path))

            size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.success(f"[완료] {output_path.name} | {size_mb:.1f} MB")
            return output_path

        except Exception as e:
            logger.error(f"[실패] 파랑 예보 다운로드: {e}")
            return None

        finally:
            if grib_path.exists():
                grib_path.unlink()

    def run(self) -> list[Path]:
        """
        ECMWF 예보 데이터 다운로드 실행 (바람 + 파랑)

        항상 최신 예보 기준으로 1회 다운로드.
        (날짜 범위 입력 불필요 — 예보는 항상 '오늘부터 10일')

        Returns
        -------
        list[Path]  →  성공적으로 저장된 파일 경로 목록
        """
        logger.info("=" * 50)
        logger.info("ECMWF HRES 예보 다운로드 시작")
        logger.info(f"  예보 기간: {self.forecast_days}일 / 스텝: {self.step_hours}시간 간격")
        logger.info("=" * 50)

        # 예보 스텝 목록 생성 (최대 step 제한 적용)
        # 예: forecast_days=10, step_hours=6 → [0, 6, 12, ..., 240]
        all_steps = _build_forecast_steps(self.step_hours)
        steps = [s for s in all_steps if s <= self.max_step_h]

        logger.info(f"  예보 스텝 수: {len(steps)}개 ({steps[0]}h ~ {steps[-1]}h)")

        downloaded = []   # 성공한 파일 경로 목록

        # tempfile.TemporaryDirectory: with 블록 종료 시 임시 폴더 자동 삭제
        with tempfile.TemporaryDirectory() as tmp_dir:

            # ── 바람 예보 다운로드 ──
            logger.info("[1/2] 바람 예보 다운로드 (u10, v10)")
            wind_path, issued_at = self._download_wind(steps, tmp_dir)
            if wind_path:
                downloaded.append(wind_path)

            # ── 파랑 예보 다운로드 ──
            # issued_at을 바람에서 받아서 재사용 (같은 예보 run 기준으로 통일)
            logger.info("[2/2] 파랑 예보 다운로드 (swh, mwd, mwp 등)")
            wave_path = self._download_wave(steps, tmp_dir, issued_at)
            if wave_path:
                downloaded.append(wave_path)

        logger.info(
            f"ECMWF 예보 다운로드 완료 | "
            f"성공: {len(downloaded)}/2 파일"
        )
        return downloaded
