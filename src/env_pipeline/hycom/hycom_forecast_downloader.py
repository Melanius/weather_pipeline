"""
HYCOM 예보 해류 데이터 다운로드 모듈
======================================

HYCOM Forecast란?
  - 기존 HYCOM 분석(expt_93.0)의 예보 버전
  - ESPC-D (Earth System Prediction Capability - D) 운영 예보 모델
  - 미 해군/NOAA 운영, OPeNDAP 무료 제공

URL 차이:
  - 분석(현재 코드): GLBy0.08/expt_93.0/uv3z      ← 과거~현재
  - 예보(이 코드):   FMRC_ESPC-D-V02_uv3z_best.ncd ← 오늘~5일 후

_best.ncd란?
  - FMRC (Forecast Model Run Collection): 여러 예보 run의 모음
  - best = 각 시각에 대해 가장 최신 예보 run의 데이터를 자동 선택
  - 별도 처리 없이 연속 시계열처럼 사용 가능

수집 범위:
  - 변수: water_u, water_v (수심 0m 해수면)
  - 시간: 오늘 00:00 UTC ~ 오늘 + 5일
  - 해상도: stride=3 → 약 0.24° (분석과 동일)

저장 경로: data/hycom/forecast/YYYY/MM/hycom_fc_current_YYYYMMDD.nc
  - YYYYMMDD = 다운로드 실행일 (issued_at)
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger


# ─────────────────────────────────────────────────────
# HYCOM 예보 OPeNDAP URL
# ─────────────────────────────────────────────────────

# FMRC Best Time Series: 각 유효 시각에 대해 최신 예보를 자동 제공
# #fillmismatch: _FillValue 타입 불일치 버그 우회 (분석과 동일)
HYCOM_FORECAST_URL = (
    "https://tds.hycom.org/thredds/dodsC/"
    "FMRC_ESPC-D-V02_uv3z/"
    "FMRC_ESPC-D-V02_uv3z_best.ncd"
    "#fillmismatch"
)

# 수집 변수 (분석과 동일)
CURRENT_VARIABLES = ["water_u", "water_v"]


class HYCOMForecastDownloader:
    """
    HYCOM 예보 해류 데이터 다운로더

    기존 HYCOMDownloader(분석)와 동일한 OPeNDAP 방식이지만
    URL과 시간 범위(미래)가 다름.

    사용 예:
        downloader = HYCOMForecastDownloader(
            output_dir=Path("data/hycom/forecast"),
            stride=3,
            forecast_days=5,
        )
        paths = downloader.run()
    """

    def __init__(self, output_dir: Path, stride: int = 3, forecast_days: int = 5):
        """
        Parameters
        ----------
        output_dir    : Path  →  NetCDF 저장 최상위 경로
        stride        : int   →  공간 해상도 조절 (3 = 0.24°)
        forecast_days : int   →  예보 기간 (일, 최대 5)
        """
        self.output_dir    = output_dir
        self.stride        = stride
        self.forecast_days = min(forecast_days, 5)   # HYCOM 예보 최대 5일

        # 저장 폴더 생성
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"HYCOMForecastDownloader 준비 완료 | "
            f"stride={stride} (약 {0.08 * stride:.2f}°) | "
            f"예보 기간: {self.forecast_days}일 | "
            f"저장 경로: {self.output_dir}"
        )

    def _get_output_path(self, issued_date: datetime) -> Path:
        """
        저장 파일 경로 생성

        경로 예: data/hycom/forecast/2026/03/hycom_fc_current_20260315.nc

        Parameters
        ----------
        issued_date : datetime  →  예보 실행일 (오늘 날짜)
        """
        # 연/월 폴더 생성
        folder = self.output_dir / f"{issued_date.year:04d}" / f"{issued_date.month:02d}"
        folder.mkdir(parents=True, exist_ok=True)

        filename = f"hycom_fc_current_{issued_date.strftime('%Y%m%d')}.nc"
        return folder / filename

    def run(self) -> list[Path]:
        """
        HYCOM 예보 해류 데이터 다운로드

        오늘 00:00 UTC ~ 오늘 + forecast_days 범위의 예보 다운로드.
        항상 최신 예보 기준으로 1회 실행.

        Returns
        -------
        list[Path]  →  성공 시 파일 경로 1개짜리 리스트, 실패 시 빈 리스트
        """
        logger.info("=" * 50)
        logger.info("HYCOM 예보 해류 다운로드 시작")
        logger.info("=" * 50)

        # ── 오늘 날짜 및 예보 범위 계산 ──
        # UTC 기준으로 오늘을 issued_at으로 사용
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
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
            f"[다운로드] HYCOM 예보 | "
            f"{forecast_start.strftime('%Y-%m-%d')} ~ "
            f"{forecast_end.strftime('%Y-%m-%d')} | "
            f"stride={self.stride}"
        )

        try:
            # ── 1단계: OPeNDAP URL로 원격 예보 데이터셋 열기 ──
            # 메타정보(시간 범위, 좌표 등)만 먼저 가져옴 (실제 데이터는 아직 미전송)
            logger.debug("  HYCOM 예보 URL 연결 중...")
            ds = xr.open_dataset(HYCOM_FORECAST_URL, drop_variables=["tau"])

            # ── 2단계: 예보 시간 범위 선택 ──
            # 오늘 00:00 UTC부터 forecast_days 이후까지
            day_start = forecast_start.strftime("%Y-%m-%dT00:00:00")
            # forecast_end = issued_at + N일 = "N+1일째 자정(00:00)"
            # 여기서 1초를 빼면 "N일째 23:59:59" → 정확히 N일치만 선택됨
            # (1초를 빼지 않으면 N+1일째 23:59:59까지 포함되어 1일 초과)
            day_end   = (forecast_end - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")

            # HYCOM FMRC Best의 변수별 시간 차원 불일치 처리
            #
            # HYCOM FMRC 데이터셋에서는 변수마다 다른 시간 차원을 사용할 수 있음:
            #   water_u → 'time'  (3시간 간격 출력)
            #   water_v → 'time1' (1시간 간격 출력, 최대 5일 = 121스텝)
            # ds[vars].sel(time=slice(...))은 'time' 차원이 없는 변수(time1 사용)에는
            # 필터가 적용되지 않아 water_v가 121스텝 전체로 저장되는 OOM 버그 발생.
            #
            # 해결: 각 변수를 독립적으로 해당 시간 차원으로 선택 후 'time'으로 통일,
            # xr.merge(join="inner")로 공통 시각(3시간 간격)만 남김.
            var_datasets = []
            for var in CURRENT_VARIABLES:
                da        = ds[var]
                # 이 변수의 시간 차원 이름 탐색 (time, time1, time2, ...)
                time_dim  = next(
                    (d for d in da.dims if d.startswith("time")), None
                )
                if time_dim is None:
                    logger.warning(f"  {var}: 시간 차원을 찾을 수 없음 → 건너뜀")
                    continue
                # 해당 시간 차원으로 날짜 범위 선택
                var_ds = da.sel({time_dim: slice(day_start, day_end)}).to_dataset(name=var)
                # time1 등 비표준 이름을 'time'으로 통일 (merge를 위해)
                if time_dim != "time":
                    var_ds = var_ds.rename({time_dim: "time"})
                    logger.debug(
                        f"  {var}: 시간 차원 '{time_dim}' → 'time' 통일 "
                        f"({len(var_ds.time)}스텝)"
                    )
                var_datasets.append(var_ds)

            # 공통 시각만 교집합(inner)으로 병합
            # water_u(3h) ∩ water_v(1h) → 3시간 간격 스텝만 남음
            ds_fc = xr.merge(var_datasets, join="inner")

            # 선택된 시간 스텝 수 확인 (3시간 간격 × 5일 × 8 = 최대 40스텝)
            time_count = len(ds_fc.time)
            logger.info(f"  예보 시간 스텝: {time_count}개")

            if time_count == 0:
                logger.warning(
                    "  예보 데이터가 없습니다. HYCOM 예보 URL 또는 시간 범위를 확인하세요."
                )
                ds.close()
                return []

            # ── 3단계: 수심 0m (해수면) 선택 ──
            # depth 차원의 index=0이 수심 0m (해수면)
            if "depth" in ds_fc.dims:
                ds_fc = ds_fc.isel(depth=0)

            # ── 4단계: stride 적용 (해상도 조절) ──
            # stride=3 → 0.08° × 3 = 0.24° (분석과 동일 해상도)
            ds_fc = ds_fc.isel(
                lat=slice(None, None, self.stride),
                lon=slice(None, None, self.stride),
            )

            # ── 5단계: 경도 변환 0~360 → -180~180 ──
            # HYCOM 예보도 분석과 마찬가지로 0~360 경도 사용
            if float(ds_fc.lon.values.max()) > 180:
                ds_fc = ds_fc.assign_coords(
                    lon=((ds_fc.lon + 180) % 360 - 180)
                ).sortby("lon")
                logger.debug("  경도 변환 완료: 0~360 → -180~180")

            # ── 6단계: 실제 데이터 메모리에 로드 ──
            # 이 시점에 실제 네트워크 전송 발생 (수 초 ~ 수십 초 소요)
            logger.debug("  예보 데이터 전송 중... (수 초 ~ 수십 초 소요)")
            ds_fc = ds_fc.load()

            # ── 7단계: issued_at 전역 속성 저장 ──
            # DB 적재 시 loader.py가 이 값을 issued_at 컬럼에 사용
            ds_fc.attrs["issued_at"] = issued_at.isoformat()
            ds_fc.attrs["forecast_days"] = self.forecast_days
            ds_fc.attrs["source"] = "HYCOM ESPC-D-V02 Forecast"

            # ── 8단계: float32 변환 + zlib 압축 후 NetCDF 저장 ──
            # HYCOM FMRC Best URL은 float64로 데이터를 제공 → 파일 크기 과다
            # float32로 변환 시 파일 크기 약 1/2, 정밀도 손실은 해양 실용 범위 내
            for var in list(ds_fc.data_vars):
                if ds_fc[var].dtype == "float64":
                    # float64 → float32 변환 (attrs 보존)
                    ds_fc[var] = ds_fc[var].astype("float32")

            # zlib 압축 인코딩: 분석 파일(~130 MB) 수준으로 압축 목표
            # complevel=4: 속도와 압축률의 균형점 (1=빠름·덜압축, 9=느림·최대압축)
            encoding = {
                var: {"zlib": True, "complevel": 4, "dtype": "float32"}
                for var in ds_fc.data_vars
            }
            ds_fc.to_netcdf(str(output_path), encoding=encoding)
            ds.close()   # 원격 연결 닫기

            size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.success(
                f"[완료] {output_path.name} | "
                f"{size_mb:.1f} MB | "
                f"시간 스텝 {time_count}개"
            )
            return [output_path]

        except Exception as e:
            logger.error(f"[실패] HYCOM 예보 다운로드: {e}")
            # 실패로 생긴 불완전한 파일 삭제
            if output_path.exists():
                output_path.unlink()
            try:
                ds.close()
            except Exception:
                pass
            return []
