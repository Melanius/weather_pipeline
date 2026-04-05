"""
환경 데이터 수집/적재 파이프라인 오케스트레이터 (Phase 10)
=============================================================

Phase 10 동작 방식:
  - pipeline_coverage 테이블로 날짜별·소스별 적재 상태 추적
  - 누락 날짜 자동 감지 → 백필 다운로드 → DB 적재
  - 재분석 complete 날짜의 예보 데이터 자동 삭제 (예보→재분석 교체)
  - HYCOM 롤링 윈도우(10일) 초과 날짜 permanent_forecast 처리
  - --simulate-date, --dry-run, --manual 플래그 지원

실행 모드:
  auto             → 자동 일일 파이프라인 (기본값)
                     coverage 테이블 기반 누락 날짜 자동 감지 + 예보 포함
  download_only    → 다운로드만 (DB/Docker 불필요)
                     로컬 파일 스캔으로 누락 감지 OR --manual 로 날짜 범위 지정
  load_only        → 로컬 .nc 파일 → DB 적재만 (ECMWF + HYCOM 전체, 다운로드 생략)
  load_hycom_only  → HYCOM 해류 파일만 DB 적재 (ECMWF 건너뜀, 다운로드 생략)
  forecast_only    → 예보 다운로드 + DB 적재만 (재분석 생략)

하위 호환 모드 (Phase 9 이전):
  full               → auto 와 동일
  full_with_forecast → auto 와 동일
  forecast           → forecast_only 와 동일
  forecast_download_only → 예보 다운로드만 (DB 적재 없음)
"""

import re
import tomllib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

from .ecmwf.era5_downloader import ERA5Downloader, load_date_range
from .ecmwf.ecmwf_forecast_downloader import ECMWFForecastDownloader
from .hycom.hycom_downloader import HYCOMDownloader
from .hycom.hycom_forecast_downloader import HYCOMForecastDownloader
from .noaa.noaa_forecast_downloader import NOAAForecastDownloader
from .db.schema import initialize_schema
from .db.loader import load_netcdf_to_db, load_multiple_files
from .db.connection import get_connection
from .db.coverage import (
    update_coverage,
    get_backfill_dates,
    cleanup_superseded_forecasts,
    check_and_promote_hycom_permanent,
    alert_long_missing,
    STATUS_COMPLETE,
    STATUS_PARTIAL,
    STATUS_FAILED,
    STATUS_FORECAST_ONLY,
    SOURCE_ECMWF_REANALYSIS,
    SOURCE_HYCOM_CURRENT,
    SOURCE_ECMWF_FORECAST,
    SOURCE_NOAA_FORECAST,
    SOURCE_HYCOM_FORECAST,
)


# ─────────────────────────────────────────────────────
# 설정 로드
# ─────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────
# 내부 헬퍼 함수
# ─────────────────────────────────────────────────────

def _to_datetime_utc(d: date) -> datetime:
    """date → datetime UTC 자정 (다운로더에 전달 시 datetime 타입 필요)"""
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _date_from_nc_path(nc_path: Path) -> date | None:
    """
    파일명에서 날짜 추출 (YYYYMMDD 패턴).
    예: ecmwf_wind_20260310.nc → date(2026, 3, 10)
    파싱 실패 시 None 반환.
    """
    m = re.search(r'(\d{8})', nc_path.stem)
    if m:
        try:
            return datetime.strptime(m.group(1), '%Y%m%d').date()
        except ValueError:
            pass
    return None


def _nc_path_for_ecmwf(base_dir: Path, d: date, data_type: str) -> Path:
    """
    ECMWF 재분석 파일 저장 경로 계산
    예: data/ecmwf/reanalysis/2026/03/ecmwf_wind_20260310.nc
    """
    return (
        base_dir
        / f"{d.year:04d}"
        / f"{d.month:02d}"
        / f"ecmwf_{data_type}_{d.strftime('%Y%m%d')}.nc"
    )


def _nc_path_for_hycom(base_dir: Path, d: date) -> Path:
    """
    HYCOM 분석 파일 저장 경로 계산
    예: data/hycom/current/2026/03/hycom_current_20260310.nc
    """
    return (
        base_dir
        / f"{d.year:04d}"
        / f"{d.month:02d}"
        / f"hycom_current_{d.strftime('%Y%m%d')}.nc"
    )


def _load_ecmwf_day_to_db(
    conn,
    target_date: date,
    wind_path: Path | None,
    wave_path: Path | None,
    batch_size: int,
    dry_run: bool,
) -> None:
    """
    ECMWF 하루치 wind + wave 파일을 DB에 적재하고 pipeline_coverage를 업데이트.

    wind와 wave 두 파일이 같은 테이블(env_ecmwf_reanalysis)에 적재됨:
      - wind 파일: u10, v10 컬럼 INSERT (wind 컬럼만 갱신, 해양 격자만 필터링)
      - wave 파일: swh 등 9개 파랑 컬럼 UPDATE (ON CONFLICT DO UPDATE)

    완료 판정 기준 (Phase 14-A):
      - wind + wave 모두 예외 없이 성공 → complete
      - 한 파일만 성공 → partial
      - 둘 다 실패 or 파일 없음 → failed

    Parameters
    ----------
    conn        : psycopg2 연결 (coverage 업데이트용)
    target_date : 처리 대상 날짜
    wind_path   : ecmwf_wind_YYYYMMDD.nc 경로 (없으면 None)
    wave_path   : ecmwf_wave_YYYYMMDD.nc 경로 (없으면 None)
    batch_size  : DB COPY 배치 크기
    dry_run     : True 이면 DB 적재 없이 로그만 출력
    """
    # ── dry_run: 파일 존재 여부만 확인하고 DB 변경 없이 반환 ──
    if dry_run:
        wind_exists = bool(wind_path and wind_path.exists() and wind_path.stat().st_size > 0)
        wave_exists = bool(wave_path and wave_path.exists() and wave_path.stat().st_size > 0)
        logger.info(
            f"[DRY-RUN] ECMWF 적재 건너뜀: {target_date} "
            f"(wind={'있음' if wind_exists else '없음'}, "
            f"wave={'있음' if wave_exists else '없음'})"
        )
        return

    rows_wind = 0
    rows_wave = 0
    wind_ok   = False
    wave_ok   = False

    # ── wind 파일 적재 ──
    if wind_path and wind_path.exists() and wind_path.stat().st_size > 0:
        try:
            rows_wind = load_netcdf_to_db(wind_path, batch_size=batch_size)
            wind_ok   = True
            logger.debug(f"  wind 적재 완료: {rows_wind:,}행")
        except Exception as e:
            logger.error(f"  wind 적재 실패 [{target_date}]: {e}")
    else:
        logger.warning(f"  wind 파일 없음 또는 빈 파일: {target_date}")

    # ── wave 파일 적재 ──
    if wave_path and wave_path.exists() and wave_path.stat().st_size > 0:
        try:
            rows_wave = load_netcdf_to_db(wave_path, batch_size=batch_size)
            wave_ok   = True
            logger.debug(f"  wave 적재 완료: {rows_wave:,}행")
        except Exception as e:
            logger.error(f"  wave 적재 실패 [{target_date}]: {e}")
    else:
        logger.warning(f"  wave 파일 없음 또는 빈 파일: {target_date}")

    # ── 상태 결정: 예외 없이 성공 여부 기준 ──
    if wind_ok and wave_ok:
        # 두 파일 모두 예외 없이 성공
        status    = STATUS_COMPLETE
        row_count = rows_wind  # wind 행 수 기록 (해양 격자 수)
    elif wind_ok or wave_ok:
        # 한 파일만 성공
        status    = STATUS_PARTIAL
        row_count = rows_wind if wind_ok else rows_wave
        logger.warning(
            f"  [{target_date}] ECMWF partial: "
            f"wind={'OK' if wind_ok else 'FAIL'}, wave={'OK' if wave_ok else 'FAIL'}"
        )
    else:
        # 둘 다 실패
        status    = STATUS_FAILED
        row_count = 0

    # coverage 갱신 및 커밋 (pipeline.py가 연결을 소유하므로 여기서 커밋)
    update_coverage(conn, target_date, SOURCE_ECMWF_REANALYSIS, status, row_count=row_count)
    conn.commit()

    if status == STATUS_COMPLETE:
        logger.success(f"  [{target_date}] ECMWF 재분석 적재 완료 (wind {rows_wind:,}행)")
    elif status == STATUS_PARTIAL:
        logger.warning(f"  [{target_date}] ECMWF 재분석 partial 적재")
    else:
        logger.error(f"  [{target_date}] ECMWF 재분석 적재 실패")


def _load_hycom_day_to_db(
    conn,
    target_date: date,
    hycom_path: Path | None,
    batch_size: int,
    dry_run: bool,
) -> None:
    """
    HYCOM 하루치 파일을 DB에 적재하고 pipeline_coverage를 업데이트.

    완료 판정 기준 (Phase 14-A):
      - 예외 없이 적재 성공 (rows > 0) → complete
      - 파일 없음 또는 빈 파일 → failed
      - 예외 발생 → failed

    Parameters
    ----------
    conn        : psycopg2 연결 (coverage 업데이트용)
    target_date : 처리 대상 날짜
    hycom_path  : hycom_current_YYYYMMDD.nc 경로 (없으면 None)
    batch_size  : DB COPY 배치 크기
    dry_run     : True 이면 DB 적재 없이 로그만 출력
    """
    # ── dry_run: 파일 존재 여부만 확인하고 DB 변경 없이 반환 ──
    if dry_run:
        hycom_exists = bool(
            hycom_path and hycom_path.exists() and hycom_path.stat().st_size > 0
        )
        logger.info(
            f"[DRY-RUN] HYCOM 적재 건너뜀: {target_date} "
            f"({'파일 있음' if hycom_exists else '파일 없음'})"
        )
        return

    # 파일이 없거나 비어있는 경우
    if not hycom_path or not hycom_path.exists() or hycom_path.stat().st_size == 0:
        logger.warning(f"  HYCOM 파일 없음 또는 빈 파일: {target_date}")
        update_coverage(
            conn, target_date, SOURCE_HYCOM_CURRENT, STATUS_FAILED,
            notes="파일 없음 또는 빈 파일"
        )
        conn.commit()
        return

    try:
        rows = load_netcdf_to_db(hycom_path, batch_size=batch_size)
        update_coverage(conn, target_date, SOURCE_HYCOM_CURRENT, STATUS_COMPLETE, row_count=rows)
        conn.commit()
        logger.success(f"  [{target_date}] HYCOM 해류 적재 완료 ({rows:,}행)")

    except Exception as e:
        logger.error(f"  HYCOM 적재 실패 [{target_date}]: {e}")
        update_coverage(
            conn, target_date, SOURCE_HYCOM_CURRENT, STATUS_FAILED,
            notes=str(e)[:200]
        )
        conn.commit()


# ─────────────────────────────────────────────────────
# 메인 파이프라인
# ─────────────────────────────────────────────────────

def run_pipeline(
    mode: str = "auto",
    today_override: date | None = None,
    dry_run: bool = False,
    manual_mode: bool = False,
    forecast_days_override: int | None = None,
) -> None:
    """
    전체 파이프라인 실행

    Parameters
    ----------
    mode : str
        "auto"                 → 자동 일일 파이프라인 (기본값, Phase 10)
                                 coverage 기반 누락 날짜 자동 감지 + 예보 포함
        "download_only"        → 다운로드만 (DB/Docker 불필요)
                                 --manual 없으면 로컬 파일 스캔으로 누락 감지
        "load_only"            → 로컬 .nc 파일 → DB 적재만 (ECMWF + HYCOM 전체, 다운로드 생략)
        "load_hycom_only"      → HYCOM 해류 파일만 DB 적재 (ECMWF 건너뜀, 다운로드 생략)
        "forecast_only"        → 예보 다운로드 + DB 적재만

        하위 호환 (Phase 9 이전):
        "full"                 → "auto" 와 동일
        "full_with_forecast"   → "auto" 와 동일
        "forecast"             → "forecast_only" 와 동일
        "forecast_download_only" → 예보 다운로드만 (DB 적재 없음)

    today_override : date | None
        None 이면 오늘 날짜(date.today()) 사용.
        --simulate-date YYYY-MM-DD 로 주입하면 그 날짜를 '오늘'로 취급.
        빠른 다중 주기 테스트 목적.

    dry_run : bool
        True 이면 DB 적재 없이 로그만 출력. 다운로드는 실제로 실행됨.

    manual_mode : bool
        True 이면 config/down.json 의 manual_start/manual_end 날짜 범위 사용.
        False(기본)이면 pipeline_coverage 기반 자동 감지.

    forecast_days_override : int | None
        None 이면 settings.toml 값 사용.
        정수 지정 시 그 값으로 예보 기간 오버라이드 (테스트용).
    """
    # ── 하위 호환: 구 모드명 정규화 ──
    mode = {
        "full":                   "auto",         # full = auto
        "full_with_forecast":     "auto",         # full_with_forecast = auto
        "forecast":               "forecast_only",  # forecast = forecast_only
    }.get(mode, mode)

    logger.info(
        f"파이프라인 시작 | 모드: {mode} | "
        f"dry_run: {dry_run} | manual: {manual_mode}"
    )

    # ── 경로 설정 ──
    project_root = Path(__file__).parent.parent.parent   # 프로젝트 루트
    config_path  = project_root / "config" / "settings.toml"
    json_path    = project_root / "config" / "down.json"   # --manual 전용

    # settings.toml 로드
    config = load_config(config_path)

    # ── 재분석 관련 경로 ──
    ecmwf_dir = project_root / config["paths"]["ecmwf_reanalysis_dir"]
    hycom_dir = project_root / config["paths"]["hycom_current_dir"]

    # ── 예보 관련 경로 ──
    ecmwf_fc_dir = project_root / config["paths"]["ecmwf_forecast_dir"]
    hycom_fc_dir = project_root / config["paths"]["hycom_forecast_dir"]
    noaa_fc_dir  = project_root / config["paths"]["noaa_forecast_dir"]

    # ── 공통 설정 ──
    ecmwf_resolution = config["ecmwf"]["spatial_resolution"]    # 0.25°
    hycom_stride     = config["hycom"]["stride"]                 # 3
    batch_size       = config["database"]["batch_size"]          # 50000
    fc_step_hours    = config["ecmwf_forecast"]["step_hours"]    # 6

    # ── pipeline 설정 (settings.toml [pipeline] 섹션) ──
    pipeline_cfg      = config.get("pipeline", {})
    lookback_days     = pipeline_cfg.get("coverage_lookback_days", 30)
    era5_delay_days   = pipeline_cfg.get("era5_delay_days", 7)
    hycom_window_days = pipeline_cfg.get("hycom_window_days", 10)
    alert_threshold   = pipeline_cfg.get("alert_missing_days_threshold", 3)
    # partial_threshold 는 Phase 14-A에서 제거됨 (에러 기반 판정으로 변경)

    # reanalysis_start_date: 재분석 수집 최초 시작일 (고정 하한선)
    # settings.toml에 없으면 None → 하한선 없이 lookback_days 그대로 적용
    _start_str = pipeline_cfg.get("reanalysis_start_date", None)
    reanalysis_start_date = (
        date.fromisoformat(_start_str) if _start_str else None
    )

    # ── 예보 기간 설정 ──
    if forecast_days_override is not None:
        # 테스트용 오버라이드: 지정한 일수만큼만 예보 수집
        fc_days       = forecast_days_override
        hycom_fc_days = min(forecast_days_override, 5)   # HYCOM 최대 5일 제한
        noaa_fc_days  = min(forecast_days_override, 7)   # NOAA 최대 7일 제한
        logger.info(
            f"예보 기간 오버라이드: ECMWF {fc_days}일 / "
            f"HYCOM {hycom_fc_days}일 / NOAA {noaa_fc_days}일"
        )
    else:
        fc_days       = config["ecmwf_forecast"]["forecast_days"]    # 10
        hycom_fc_days = config["hycom_forecast"]["forecast_days"]     # 5
        noaa_fc_days  = config["noaa_forecast"]["forecast_days"]      # 5

    # ── 기준 날짜 설정 ──
    today = today_override or date.today()
    if today_override:
        logger.info(f"[시뮬레이션] 기준 날짜: {today} (실제 오늘: {date.today()})")
    else:
        logger.info(f"기준 날짜: {today}")

    # ── 모드별 실행 플래그 ──
    need_db         = mode not in ("download_only", "forecast_download_only")
    run_reanalysis  = mode in ("auto", "download_only", "load_only", "load_hycom_only")
    run_forecast    = mode in ("auto", "forecast_only", "forecast_download_only")
    forecast_need_db = need_db and mode != "forecast_download_only"

    # ── DB 스키마 초기화 ──
    if need_db and not dry_run:
        logger.info("DB 스키마 확인 중...")
        initialize_schema()

    # ════════════════════════════════════════════════════
    # 재분석 파이프라인
    # ════════════════════════════════════════════════════
    if run_reanalysis:
        logger.info("=" * 55)
        logger.info("재분석 파이프라인 시작")
        logger.info("=" * 55)

        # ── STEP 1: 다운로드 대상 날짜 결정 ──
        ecmwf_dates: list[date] = []   # ECMWF 재분석 처리 대상 날짜
        hycom_dates:  list[date] = []  # HYCOM 분석 처리 대상 날짜

        if mode == "load_only":
            # load_only: 로컬 wind 파일 목록에서 날짜 추출 (다운로드 없음)
            wind_files  = sorted(ecmwf_dir.rglob("ecmwf_wind_*.nc"))
            hycom_files = sorted(hycom_dir.rglob("hycom_current_*.nc"))
            # 날짜 파싱 실패(None) 제외
            ecmwf_dates = [d for f in wind_files if (d := _date_from_nc_path(f))]
            hycom_dates = [d for f in hycom_files if (d := _date_from_nc_path(f))]
            logger.info(
                f"load_only 스캔 결과: "
                f"ECMWF {len(ecmwf_dates)}일 / HYCOM {len(hycom_dates)}일 적재 예정"
            )

        elif mode == "load_hycom_only":
            # load_hycom_only: HYCOM 해류 파일만 적재, ECMWF 완전 건너뜀
            # --manual 플래그 사용 시 down.json 날짜 범위로 제한
            # --manual 없으면 로컬 HYCOM 파일 전체 스캔
            if manual_mode:
                start_dt, end_dt = load_date_range(json_path)
                start_d, end_d   = start_dt.date(), end_dt.date()
                date_range = [
                    start_d + timedelta(days=i)
                    for i in range((end_d - start_d).days + 1)
                ]
                # 실제로 파일이 존재하는 날짜만 필터링
                hycom_dates = [
                    d for d in date_range
                    if _nc_path_for_hycom(hycom_dir, d).exists()
                    and _nc_path_for_hycom(hycom_dir, d).stat().st_size > 0
                ]
                logger.info(
                    f"load_hycom_only (manual): {start_d} ~ {end_d} 범위 중 "
                    f"파일 있는 {len(hycom_dates)}일 적재 예정"
                )
            else:
                hycom_files = sorted(hycom_dir.rglob("hycom_current_*.nc"))
                hycom_dates = [d for f in hycom_files if (d := _date_from_nc_path(f))]
                logger.info(
                    f"load_hycom_only 스캔 결과: HYCOM {len(hycom_dates)}일 적재 예정 "
                    f"(ECMWF 건너뜀)"
                )
            ecmwf_dates = []  # ECMWF 적재 완전 생략

        elif manual_mode:
            # 수동 모드: down.json 의 manual_start/manual_end 범위 사용
            start_dt, end_dt = load_date_range(json_path)
            start_d, end_d   = start_dt.date(), end_dt.date()
            date_range = [
                start_d + timedelta(days=i)
                for i in range((end_d - start_d).days + 1)
            ]
            ecmwf_dates = list(date_range)
            hycom_dates = list(date_range)
            logger.info(
                f"수동 백필 날짜 범위: {start_d} ~ {end_d} ({len(ecmwf_dates)}일)"
            )

        elif mode == "download_only":
            # download_only (DB 불필요): 로컬 파일 스캔으로 누락 날짜 감지
            range_start = today - timedelta(days=lookback_days)
            range_end   = today - timedelta(days=era5_delay_days)

            # ECMWF: wind AND wave 두 파일 모두 있어야 완전 → 하나라도 없으면 누락
            ecmwf_dates = []
            hycom_dates = []
            current = range_start
            while current <= range_end:
                wind_p = _nc_path_for_ecmwf(ecmwf_dir, current, "wind")
                wave_p = _nc_path_for_ecmwf(ecmwf_dir, current, "wave")
                wind_ok = wind_p.exists() and wind_p.stat().st_size > 0
                wave_ok = wave_p.exists() and wave_p.stat().st_size > 0
                if not (wind_ok and wave_ok):
                    ecmwf_dates.append(current)

                hycom_p = _nc_path_for_hycom(hycom_dir, current)
                if not (hycom_p.exists() and hycom_p.stat().st_size > 0):
                    hycom_dates.append(current)

                current += timedelta(days=1)

            logger.info(
                f"로컬 스캔 누락 감지: "
                f"ECMWF {len(ecmwf_dates)}일 / HYCOM {len(hycom_dates)}일 "
                f"(범위: {range_start} ~ {range_end})"
            )

        else:
            # auto 모드: pipeline_coverage 테이블 기반으로 누락 날짜 조회
            conn_cov = get_connection()
            try:
                # HYCOM 롤링 윈도우 초과 날짜 permanent_forecast 처리 (먼저 실행)
                promoted = check_and_promote_hycom_permanent(
                    conn_cov, today, hycom_window_days
                )
                if promoted:
                    logger.info(f"HYCOM permanent_forecast 승격: {len(promoted)}일")

                # coverage 기반 백필 대상 날짜 조회
                # reanalysis_start_date: settings.toml의 고정 하한선 전달
                backfill    = get_backfill_dates(
                    conn_cov, today, lookback_days, era5_delay_days,
                    reanalysis_start_date=reanalysis_start_date,
                )
                ecmwf_dates = backfill[SOURCE_ECMWF_REANALYSIS]
                hycom_dates = backfill[SOURCE_HYCOM_CURRENT]

            finally:
                conn_cov.close()

        # ── STEP 2: ECMWF ERA5 재분석 다운로드 + 적재 ──
        if ecmwf_dates:
            logger.info(f"ECMWF 재분석 처리 시작: {len(ecmwf_dates)}일")
            logger.info("-" * 40)

            # download_only를 제외한 모드에서 ERA5Downloader 인스턴스 생성
            # load_only / load_hycom_only 는 다운로드 없이 적재만
            ecmwf_downloader = (
                ERA5Downloader(output_dir=ecmwf_dir)
                if mode not in ("load_only", "load_hycom_only")
                else None
            )

            for d in ecmwf_dates:
                dt = _to_datetime_utc(d)

                # 다운로드 (load_only가 아니고 dry_run도 아닌 경우)
                # dry_run=True 이면 다운로드 없이 의도만 로그 출력
                if ecmwf_downloader is not None:
                    if dry_run:
                        logger.info(
                            f"[DRY-RUN] ECMWF 다운로드 건너뜀: "
                            f"{d.strftime('%Y-%m-%d')} wind/wave"
                        )
                    else:
                        ecmwf_downloader.download_day(dt, "wind", ecmwf_resolution)
                        ecmwf_downloader.download_day(dt, "wave", ecmwf_resolution)

                # DB 적재 + coverage 업데이트 (download_only가 아닌 경우)
                if need_db:
                    wind_path = _nc_path_for_ecmwf(ecmwf_dir, d, "wind")
                    wave_path = _nc_path_for_ecmwf(ecmwf_dir, d, "wave")
                    conn_cov  = get_connection()
                    try:
                        _load_ecmwf_day_to_db(
                            conn_cov, d,
                            wind_path, wave_path,
                            batch_size, dry_run,
                        )
                    finally:
                        conn_cov.close()
        else:
            logger.info("ECMWF 재분석: 처리 대상 날짜 없음")

        # ── STEP 3: HYCOM 분석 해류 다운로드 + 적재 ──
        if hycom_dates:
            logger.info(f"HYCOM 분석 해류 처리 시작: {len(hycom_dates)}일")
            logger.info("-" * 40)

            # load_only / load_hycom_only 는 다운로드 없이 적재만
            hycom_downloader = (
                HYCOMDownloader(output_dir=hycom_dir, stride=hycom_stride)
                if mode not in ("load_only", "load_hycom_only")
                else None
            )

            for d in hycom_dates:
                dt = _to_datetime_utc(d)

                # 다운로드 (load_only가 아니고 dry_run도 아닌 경우)
                if hycom_downloader is not None:
                    if dry_run:
                        logger.info(
                            f"[DRY-RUN] HYCOM 다운로드 건너뜀: "
                            f"{d.strftime('%Y-%m-%d')}"
                        )
                    else:
                        hycom_downloader.download_day(dt)

                # DB 적재 + coverage 업데이트
                if need_db:
                    hycom_path = _nc_path_for_hycom(hycom_dir, d)
                    conn_cov   = get_connection()
                    try:
                        _load_hycom_day_to_db(
                            conn_cov, d,
                            hycom_path,
                            batch_size, dry_run,
                        )
                    finally:
                        conn_cov.close()
        else:
            logger.info("HYCOM 해류: 처리 대상 날짜 없음")

        # ── STEP 4: 예보 cleanup + 장기 누락 경고 ──
        if need_db:
            conn_cov = get_connection()
            try:
                # 재분석 complete 날짜의 예보 데이터 삭제 (예보 → 재분석 교체)
                cleanup_superseded_forecasts(conn_cov, dry_run=dry_run)

                # N일 이상 missing/failed 경고
                alert_long_missing(conn_cov, today, alert_threshold)

            finally:
                conn_cov.close()

    # ════════════════════════════════════════════════════
    # 예보 파이프라인
    # ════════════════════════════════════════════════════
    if run_forecast:
        logger.info("=" * 55)
        logger.info("예보 파이프라인 시작")
        logger.info("=" * 55)

        if dry_run:
            # dry_run: 예보 다운로드 및 적재 모두 건너뜀
            logger.info("[DRY-RUN] ECMWF 예보 다운로드 건너뜀")
            logger.info("[DRY-RUN] HYCOM 예보 다운로드 건너뜀")
            logger.info("[DRY-RUN] NOAA 예보 다운로드 건너뜀")
            logger.info("[DRY-RUN] 예보 DB 적재 건너뜀")
        else:
            # ── ECMWF HRES 예보 다운로드 ──
            logger.info("ECMWF HRES 예보 다운로드 시작")
            ecmwf_fc_downloader = ECMWFForecastDownloader(
                output_dir=ecmwf_fc_dir,
                forecast_days=fc_days,
                step_hours=fc_step_hours,
            )
            ecmwf_fc_paths = ecmwf_fc_downloader.run()

            # ── HYCOM 예보 해류 다운로드 ──
            logger.info("HYCOM 예보 해류 다운로드 시작")
            hycom_fc_downloader = HYCOMForecastDownloader(
                output_dir=hycom_fc_dir,
                stride=hycom_stride,
                forecast_days=hycom_fc_days,
            )
            hycom_fc_paths = hycom_fc_downloader.run()

            # ── NOAA WW3 파랑 예보 다운로드 ──
            # ECMWF Open Data는 너울/풍파 분리 변수를 제공하지 않으므로
            # NOAA WaveWatch III(PacIOOS)에서 파랑 9개 변수를 수집
            logger.info("NOAA WW3 파랑 예보 다운로드 시작")
            noaa_fc_downloader = NOAAForecastDownloader(
                output_dir=noaa_fc_dir,
                forecast_days=noaa_fc_days,
            )
            noaa_fc_paths = noaa_fc_downloader.run()

            # ── 예보 DB 적재 + pipeline_coverage 기록 (Phase 14-C) ──
            if forecast_need_db:
                # 소스별로 개별 적재 → 각각 pipeline_coverage 기록
                # 성공: STATUS_FORECAST_ONLY / 실패: STATUS_FAILED
                fc_source_map = [
                    (ecmwf_fc_paths, SOURCE_ECMWF_FORECAST,  "ECMWF 바람 예보"),
                    (hycom_fc_paths, SOURCE_HYCOM_FORECAST,   "HYCOM 해류 예보"),
                    (noaa_fc_paths,  SOURCE_NOAA_FORECAST,    "NOAA WW3 파랑 예보"),
                ]
                conn_cov = get_connection()
                try:
                    for fc_paths, source, label in fc_source_map:
                        if not fc_paths:
                            logger.warning(f"{label}: 다운로드된 파일 없음 → coverage=failed 기록")
                            update_coverage(
                                conn_cov, today, source, STATUS_FAILED,
                                notes="다운로드 파일 없음"
                            )
                            conn_cov.commit()
                            continue

                        logger.info(f"{label} DB 적재 시작: {len(fc_paths)}개 파일")
                        try:
                            result = load_multiple_files(fc_paths, batch_size=batch_size)
                            if result["success"] > 0:
                                update_coverage(
                                    conn_cov, today, source, STATUS_FORECAST_ONLY,
                                    row_count=result["total_rows"],
                                )
                                logger.success(
                                    f"{label} 적재 완료 | "
                                    f"성공: {result['success']}파일 / "
                                    f"총 {result['total_rows']:,}행"
                                )
                            else:
                                update_coverage(
                                    conn_cov, today, source, STATUS_FAILED,
                                    notes="모든 파일 적재 실패"
                                )
                                logger.error(f"{label}: 모든 파일 적재 실패")
                        except Exception as e:
                            update_coverage(
                                conn_cov, today, source, STATUS_FAILED,
                                notes=str(e)[:200]
                            )
                            logger.error(f"{label} 적재 예외: {e}")
                        conn_cov.commit()
                finally:
                    conn_cov.close()

    logger.success(f"파이프라인 완료 | 모드: {mode} | 기준 날짜: {today}")
