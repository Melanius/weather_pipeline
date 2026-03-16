"""
HYCOM 해류 데이터 다운로드 모듈
================================

HYCOM(HYbrid Coordinate Ocean Model)이란?
  - 미 해군/NOAA에서 운영하는 전세계 해양 순환 모델
  - 해류(ocean current), 수온, 염분 등 해양 데이터 제공
  - 무료 공개 데이터 (API 키 불필요)

접근 방식: OPeNDAP (Open-source Project for a Network Data Access Protocol)
  - URL 기반으로 원격 데이터를 직접 파이썬에서 스트리밍
  - xarray가 URL을 열고, 필요한 부분만 선택 후 로컬에 저장

수집 변수:
  - water_u: 동서 방향 해류 속도 (m/s, 동쪽 +)
  - water_v: 남북 방향 해류 속도 (m/s, 북쪽 +)
  - 수심 0m (해수면) 데이터만 사용 (선박 운항 관련)

해상도: 원본 0.08° → stride=3 적용 → 약 0.24° (ERA5의 0.25°와 유사)
시간 해상도: 3시간 간격 (하루 8개 시점: 00, 03, 06, 09, 12, 15, 18, 21시)

저장 경로: data/hycom/current/YYYY/MM/hycom_current_YYYYMMDD.nc
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import xarray as xr              # NetCDF/OPeNDAP 데이터 처리 라이브러리
import numpy as np               # 수치 연산 (경도 변환 등)
from loguru import logger

# ECMWF 다운로더의 load_date_range 함수를 재사용
# (down.json 읽기 로직이 동일하므로 중복 작성 불필요)
from ..ecmwf.era5_downloader import load_date_range


# ─────────────────────────────────────────────
# HYCOM OPeNDAP URL 설정
# ─────────────────────────────────────────────

# HYCOM FMRC ESPC-D-V02 Best Time Series
# ─────────────────────────────────────────────────────────
# 기존 expt_93.0 (2018-12-04 ~ 2024-09-05)은 서비스 종료됨.
# ESPC-D-V02가 그 후속 운영 모델이며, FMRC Best 파일로 접근.
#
# FMRC (Forecast Model Run Collection) Best:
#   - 각 유효 시각에 대해 가장 최신 모델 run의 데이터를 자동 선택
#   - 과거(약 D-10일) + 현재 + 미래(약 D+5일) 데이터를 연속 시계열로 제공
#   - 예보용(hycom_forecast_downloader.py)과 동일한 URL 사용
#
# 주의: 약 최근 10일치만 보유 → 오래된 과거 데이터는 별도 아카이브 필요
HYCOM_URL = (
    "https://tds.hycom.org/thredds/dodsC/"
    "FMRC_ESPC-D-V02_uv3z/"
    "FMRC_ESPC-D-V02_uv3z_best.ncd"
    "#fillmismatch"
)

# 수집할 변수 목록 (수심 0m 해수면 해류만)
CURRENT_VARIABLES = ["water_u", "water_v"]


class HYCOMDownloader:
    """
    HYCOM 해류 데이터 다운로더

    ECMWF ERA5Downloader와 동일한 인터페이스를 유지하여
    파이프라인에서 일관되게 사용 가능.

    사용 예:
        downloader = HYCOMDownloader(output_dir=Path("data/hycom/current"), stride=3)
        downloader.run(json_path=Path("config/down.json"))
    """

    def __init__(self, output_dir: Path, stride: int = 3):
        """
        Parameters
        ----------
        output_dir : Path   →  NetCDF 파일을 저장할 최상위 디렉토리
        stride     : int    →  공간 해상도 조절용 간격
                               3 = 0.24° (ERA5의 0.25°와 유사)
                               6 = 0.48°
        """
        self.output_dir = output_dir
        self.stride = stride  # lat/lon 축에서 몇 칸 간격으로 추출할지

        # 저장 폴더가 없으면 자동 생성
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"HYCOMDownloader 준비 완료 | "
            f"stride={stride} (해상도 약 {0.08 * stride:.2f}°) | "
            f"저장 경로: {self.output_dir}"
        )

    def _get_output_path(self, date: datetime) -> Path:
        """
        저장 파일 경로 생성

        경로 예: data/hycom/current/2026/03/hycom_current_20260310.nc

        Parameters
        ----------
        date : datetime  →  대상 날짜
        """
        # 연/월 폴더 생성 (없으면 자동 생성)
        folder = self.output_dir / f"{date.year:04d}" / f"{date.month:02d}"
        folder.mkdir(parents=True, exist_ok=True)

        # 파일명: hycom_current_YYYYMMDD.nc
        filename = f"hycom_current_{date.strftime('%Y%m%d')}.nc"
        return folder / filename

    def download_day(self, date: datetime) -> Path | None:
        """
        특정 날짜 1일치 HYCOM 해류 데이터 다운로드

        동작 순서:
          1. OPeNDAP URL로 원격 데이터셋 열기 (실제 다운로드 X, 메타정보만)
          2. 해당 날짜 + 수심 0m + stride 적용으로 필요한 부분만 선택
          3. 실제 데이터 메모리에 로드 (이때 실제 전송 발생)
          4. 로컬 NetCDF 파일로 저장

        Parameters
        ----------
        date : datetime  →  다운로드할 날짜

        Returns
        -------
        Path | None  →  성공 시 저장 경로, 실패 시 None
        """
        output_path = self._get_output_path(date)

        # ── 이미 파일이 있으면 건너뜀 (단, 빈 파일은 재시도) ──
        if output_path.exists() and output_path.stat().st_size > 0:
            # 파일이 있어도 time=0인 빈 파일일 수 있으므로 실제 데이터 확인
            try:
                ds_check = xr.open_dataset(output_path)
                time_count = len(ds_check.get("time", []))
                ds_check.close()

                if time_count > 0:
                    # 정상 파일 → 건너뜀
                    size_mb = output_path.stat().st_size / (1024 * 1024)
                    logger.info(f"[건너뜀] {output_path.name} ({size_mb:.1f} MB, {time_count}스텝)")
                    return output_path
                else:
                    # time=0인 빈 파일 → 삭제 후 재시도
                    logger.info(
                        f"[빈 파일 재시도] {output_path.name} — "
                        f"time=0 감지, 삭제 후 재다운로드"
                    )
                    output_path.unlink()
            except Exception:
                # 파일 읽기 실패 → 삭제 후 재시도
                logger.warning(f"[손상 파일 재시도] {output_path.name} — 삭제 후 재다운로드")
                output_path.unlink()

        logger.info(
            f"[다운로드] {date.strftime('%Y-%m-%d')} "
            f"해류 2변수 / stride={self.stride} / 수심 0m"
        )

        try:
            # ── 1단계: 원격 HYCOM 데이터셋 열기 ──
            # OPeNDAP URL을 열면 실제 데이터는 아직 다운로드되지 않음
            # 좌표 정보(시간/위경도 범위 등)만 먼저 가져옴
            # drop_variables=['tau']: tau 변수는 일부 버전에서 오류 유발
            ds = xr.open_dataset(HYCOM_URL, drop_variables=["tau"])

            # ── 2단계: 해당 날짜 시간 범위 선택 ──
            # HYCOM은 3시간 간격: 00:00, 03:00, 06:00, ..., 21:00 (하루 8개)
            day_start = f"{date.strftime('%Y-%m-%d')}T00:00:00"
            day_end   = f"{date.strftime('%Y-%m-%d')}T23:59:59"

            # sel(time=slice(...)): 시작~끝 시각 범위로 시간 축 선택
            ds_day = ds[CURRENT_VARIABLES].sel(
                time=slice(day_start, day_end)
            )

            # ── 2.5단계: 시간 스텝이 0개인지 확인 ──
            # HYCOM 분석 데이터는 약 1~2일 지연이 있어
            # 요청 날짜가 서버에 아직 없으면 time=0으로 빈 데이터가 반환됨.
            # 이 경우 빈 파일을 저장하지 않고 None 반환하여 재시도 가능하게 함.
            if len(ds_day.time) == 0:
                logger.warning(
                    f"[데이터 없음] {date.strftime('%Y-%m-%d')} — "
                    f"HYCOM 서버에 해당 날짜 데이터가 아직 없습니다. "
                    f"(HYCOM 분석 데이터는 보통 1~2일 지연)"
                )
                ds.close()
                return None

            # ── 3단계: 수심 0m (해수면) 선택 ──
            # expt_93.0: depth가 40개 레벨 차원으로 존재 → isel(depth=0) 필요
            # FMRC ESPC-D-V02: depth가 스칼라 좌표(이미 0m 고정)로 존재 → 처리 불필요
            # 두 경우 모두 안전하게 동작하도록 depth가 차원일 때만 선택
            if "depth" in ds_day.dims:
                ds_day = ds_day.isel(depth=0)

            # ── 4단계: stride 적용 (해상도 조절) ──
            # isel(lat=slice(None, None, stride)): 모든 lat에서 stride 간격으로 추출
            # stride=3이면 원본의 1/3 격자점만 선택 → 0.08° × 3 ≈ 0.24°
            ds_day = ds_day.isel(
                lat=slice(None, None, self.stride),
                lon=slice(None, None, self.stride),
            )

            # ── 5단계: 경도 변환 0~360 → -180~180 ──
            # HYCOM은 경도를 0~360으로 저장 (예: 동경 130° → 130, 서경 90° → 270)
            # ECMWF는 -180~180 사용 → 일관성을 위해 변환
            # (lon + 180) % 360 - 180: 270 → -90, 130 → 130 (변환 공식)
            ds_day = ds_day.assign_coords(
                lon=((ds_day.lon + 180) % 360 - 180)
            ).sortby("lon")  # 변환 후 경도 기준으로 다시 정렬

            # ── 6단계: 실제 데이터 메모리에 로드 ──
            # 지금까지는 "무엇을 가져올지" 정의만 했음
            # .load()를 호출해야 실제 네트워크 전송이 발생
            logger.debug("  데이터 전송 중... (수 초 ~ 수십 초 소요)")
            ds_day = ds_day.load()

            # ── 7단계: 로컬 NetCDF 파일로 저장 ──
            ds_day.to_netcdf(str(output_path))
            ds.close()  # 원격 연결 닫기

            size_mb = output_path.stat().st_size / (1024 * 1024)
            time_count = len(ds_day.time)  # 하루 몇 개 시점인지 확인
            logger.success(
                f"[완료] {output_path.name} | "
                f"{size_mb:.1f} MB | "
                f"시간 {time_count}개 ({time_count * 3}시간)"
            )
            return output_path

        except Exception as e:
            logger.error(f"[실패] {date.strftime('%Y-%m-%d')}: {e}")
            # 실패로 생긴 불완전한 파일 삭제
            if output_path.exists():
                output_path.unlink()
            # 열린 연결이 있으면 닫기
            try:
                ds.close()
            except Exception:
                pass
            return None

    def run(self, json_path: Path) -> list[Path]:
        """
        down.json을 읽어 전체 날짜 범위의 해류 데이터 다운로드

        Parameters
        ----------
        json_path : Path  →  config/down.json 경로

        Returns
        -------
        list[Path]  →  성공적으로 다운로드된 파일 경로 목록
        """
        # JSON에서 시작일/종료일 로드 (ECMWF와 동일한 함수 사용)
        start_date, end_date = load_date_range(json_path)

        total_days   = (end_date - start_date).days + 1
        downloaded   = []
        current_date = start_date
        day_index    = 0

        logger.info(f"HYCOM 다운로드 시작 | 총 {total_days}일")

        # 날짜 반복: 시작일부터 종료일까지 하루씩
        while current_date <= end_date:
            day_index += 1
            logger.info(f"[{day_index}/{total_days}일] {current_date.strftime('%Y-%m-%d')}")

            path = self.download_day(current_date)
            if path is not None:
                downloaded.append(path)

            # 다음 날짜로 이동
            current_date += timedelta(days=1)

        logger.info(
            f"HYCOM 다운로드 완료 | 성공: {len(downloaded)}/{total_days}개 파일"
        )
        return downloaded
