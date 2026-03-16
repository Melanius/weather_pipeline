"""
환경 데이터 수집/적재 파이프라인 오케스트레이터
================================================

전체 흐름:

[재분석 모드] config/down.json 날짜 범위 기반
  → ECMWF ERA5/ERA5T wind/wave 다운로드
  → HYCOM 분석 해류 다운로드
  → TimescaleDB 적재

[예보 모드] 항상 최신 예보 기준 (날짜 입력 불필요)
  → ECMWF HRES 예보 wind/wave 다운로드 (10일)
  → HYCOM 예보 해류 다운로드 (5일)
  → TimescaleDB 적재

[전체 모드] 재분석 + 예보 동시 실행
"""

import tomllib          # Python 3.11+ 내장 TOML 파서 (설정 파일 읽기)
from pathlib import Path
from loguru import logger

from .ecmwf.era5_downloader import ERA5Downloader
from .ecmwf.ecmwf_forecast_downloader import ECMWFForecastDownloader      # ECMWF 예보
from .hycom.hycom_downloader import HYCOMDownloader
from .hycom.hycom_forecast_downloader import HYCOMForecastDownloader      # HYCOM 예보
from .noaa.noaa_forecast_downloader import NOAAForecastDownloader         # NOAA WW3 파랑 예보
from .db.schema import initialize_schema
from .db.loader import load_multiple_files


def load_config(config_path: Path) -> dict:
    """
    settings.toml 파일을 읽어서 딕셔너리로 반환

    Parameters
    ----------
    config_path : Path  →  설정 파일 경로
    """
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    logger.debug(f"설정 로드 완료: {config_path}")
    return config


def run_pipeline(
    mode: str = "download_only",
    forecast_days_override: int | None = None,
) -> None:
    """
    전체 파이프라인 실행

    Parameters
    ----------
    mode : str
        "download_only"          → 재분석 다운로드만 (DB 적재 생략)
        "load_only"              → 이미 다운로드된 재분석 파일을 DB에만 적재
        "full"                   → 재분석 다운로드 + DB 적재
        "forecast"               → 예보 다운로드 + DB 적재
        "forecast_download_only" → 예보 다운로드만 (DB 적재 생략)
        "full_with_forecast"     → 재분석 + 예보 다운로드 + DB 적재

    forecast_days_override : int | None
        None이면 settings.toml의 forecast_days 값 사용 (기본 동작).
        정수(1~10)를 넘기면 settings.toml 값을 무시하고 이 값으로 예보 기간 설정.
        주로 --forecast-days 1 처럼 빠른 테스트 목적으로 사용.
    """
    logger.info(f"파이프라인 시작 | 모드: {mode}")

    # ── 경로 설정 ──
    project_root = Path(__file__).parent.parent.parent  # 루트
    config_path  = project_root / "config" / "settings.toml"
    json_path    = project_root / "config" / "down.json"   # 재분석 날짜 범위

    # settings.toml 로드
    config = load_config(config_path)

    # ── 재분석 경로 및 설정 ──
    ecmwf_dir        = project_root / config["paths"]["ecmwf_reanalysis_dir"]
    hycom_dir        = project_root / config["paths"]["hycom_current_dir"]
    ecmwf_resolution = config["ecmwf"]["spatial_resolution"]   # 0.25°
    hycom_stride     = config["hycom"]["stride"]                # 3

    # ── 예보 경로 및 설정 ──
    ecmwf_fc_dir  = project_root / config["paths"]["ecmwf_forecast_dir"]
    hycom_fc_dir  = project_root / config["paths"]["hycom_forecast_dir"]
    noaa_fc_dir   = project_root / config["paths"]["noaa_forecast_dir"]   # NOAA WW3 저장 경로
    fc_step_hours = config["ecmwf_forecast"]["step_hours"]   # 6

    # forecast_days_override가 지정된 경우 settings.toml 값을 무시
    # None이면 settings.toml의 값 그대로 사용
    if forecast_days_override is not None:
        fc_days       = forecast_days_override                   # ECMWF 예보 기간 (오버라이드)
        hycom_fc_days = min(forecast_days_override, 5)           # HYCOM 최대 5일 제한
        noaa_fc_days  = min(forecast_days_override, 7)           # NOAA WW3 최대 7일 제한
        logger.info(
            f"예보 기간 오버라이드 적용 | "
            f"ECMWF: {fc_days}일 / HYCOM: {hycom_fc_days}일 / NOAA: {noaa_fc_days}일"
        )
    else:
        fc_days       = config["ecmwf_forecast"]["forecast_days"]   # 10 (settings.toml 기본값)
        hycom_fc_days = config["hycom_forecast"]["forecast_days"]    # 5
        noaa_fc_days  = config["noaa_forecast"]["forecast_days"]     # 5

    # ── 공통 설정 ──
    batch_size = config["database"]["batch_size"]   # 50000

    # ── 실행 모드 분류 ──
    # 재분석 관련 모드 여부
    is_reanalysis_mode = mode in ("download_only", "load_only", "full", "full_with_forecast")
    # 예보 관련 모드 여부
    is_forecast_mode   = mode in ("forecast", "forecast_download_only", "full_with_forecast")
    # DB 적재 필요 여부
    need_db = mode not in ("download_only", "forecast_download_only")

    logger.info(
        f"재분석 모드: {is_reanalysis_mode} | "
        f"예보 모드: {is_forecast_mode} | "
        f"DB 적재: {need_db}"
    )

    # ── DB 스키마 초기화 ──
    if need_db:
        logger.info("DB 스키마 확인 중...")
        initialize_schema()

    all_downloaded = []   # 이번 실행에서 다운로드된 파일 경로 누적

    # ════════════════════════════════════════
    # 재분석 다운로드
    # ════════════════════════════════════════
    if is_reanalysis_mode and mode != "load_only":

        # ── ECMWF ERA5/ERA5T 다운로드 (wind + wave) ──
        logger.info("=" * 50)
        logger.info("ECMWF 재분석 다운로드 시작")
        logger.info("=" * 50)
        ecmwf_downloader = ERA5Downloader(output_dir=ecmwf_dir)
        ecmwf_paths = ecmwf_downloader.run(
            json_path=json_path,
            resolution=ecmwf_resolution,
        )
        all_downloaded.extend(ecmwf_paths)

        # ── HYCOM 분석 해류 다운로드 ──
        logger.info("=" * 50)
        logger.info("HYCOM 분석 해류 다운로드 시작")
        logger.info("=" * 50)
        hycom_downloader = HYCOMDownloader(
            output_dir=hycom_dir,
            stride=hycom_stride,
        )
        hycom_paths = hycom_downloader.run(json_path=json_path)
        all_downloaded.extend(hycom_paths)

    elif mode == "load_only":
        # load_only: 재분석 폴더의 기존 .nc 파일 전부 수집
        logger.info("재분석 기존 파일 검색 중...")
        ecmwf_files = sorted(ecmwf_dir.rglob("*.nc"))
        hycom_files = sorted(hycom_dir.rglob("*.nc"))
        all_downloaded = ecmwf_files + hycom_files
        logger.info(
            f"발견된 재분석 파일: ECMWF {len(ecmwf_files)}개 + "
            f"HYCOM {len(hycom_files)}개 = 총 {len(all_downloaded)}개"
        )

    # ════════════════════════════════════════
    # 예보 다운로드
    # ════════════════════════════════════════
    if is_forecast_mode:

        # ── ECMWF HRES 예보 다운로드 (wind + wave, 10일) ──
        logger.info("=" * 50)
        logger.info("ECMWF 예보 다운로드 시작")
        logger.info("=" * 50)
        ecmwf_fc_downloader = ECMWFForecastDownloader(
            output_dir=ecmwf_fc_dir,
            forecast_days=fc_days,
            step_hours=fc_step_hours,
        )
        ecmwf_fc_paths = ecmwf_fc_downloader.run()
        all_downloaded.extend(ecmwf_fc_paths)

        # ── HYCOM 예보 해류 다운로드 (5일) ──
        logger.info("=" * 50)
        logger.info("HYCOM 예보 해류 다운로드 시작")
        logger.info("=" * 50)
        hycom_fc_downloader = HYCOMForecastDownloader(
            output_dir=hycom_fc_dir,
            stride=hycom_stride,
            forecast_days=hycom_fc_days,
        )
        hycom_fc_paths = hycom_fc_downloader.run()
        all_downloaded.extend(hycom_fc_paths)

        # ── NOAA WW3 파랑 예보 다운로드 ──
        # ECMWF Open Data에서 너울/풍파 분리 변수(6개)를 제공하지 않아
        # NOAA WaveWatch III(PacIOOS)에서 파랑 9개 변수를 완전히 수집
        logger.info("=" * 50)
        logger.info("NOAA WW3 파랑 예보 다운로드 시작")
        logger.info("=" * 50)
        noaa_fc_downloader = NOAAForecastDownloader(
            output_dir=noaa_fc_dir,
            forecast_days=noaa_fc_days,
        )
        noaa_fc_paths = noaa_fc_downloader.run()
        all_downloaded.extend(noaa_fc_paths)

    # ════════════════════════════════════════
    # DB 적재
    # ════════════════════════════════════════
    if need_db and all_downloaded:
        logger.info(f"DB 적재 시작: 총 {len(all_downloaded)}개 파일")

        # 파일명으로 테이블 자동 판단
        # ecmwf_wind_*   → env_ecmwf_reanalysis
        # ecmwf_wave_*   → env_ecmwf_reanalysis
        # hycom_current_* → env_hycom_current
        # ecmwf_fc_wind_* → env_ecmwf_forecast
        # ecmwf_fc_wave_* → env_ecmwf_forecast
        # hycom_fc_current_* → env_hycom_forecast
        result = load_multiple_files(
            nc_paths=all_downloaded,
            table_name=None,   # 파일명 자동 판단
            batch_size=batch_size,
        )

        logger.success(
            f"파이프라인 완료 | "
            f"성공: {result['success']}파일 | "
            f"총 {result['total_rows']:,}행"
        )

    elif mode in ("download_only", "forecast_download_only"):
        logger.info("다운로드 전용 모드 — DB 적재 건너뜀")
    elif not all_downloaded:
        logger.warning("적재할 파일이 없습니다.")
