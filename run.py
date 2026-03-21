"""
환경 데이터 파이프라인 실행 진입점 (Phase 10)
==============================================

Phase 10 신규 플래그:
  --simulate-date YYYY-MM-DD   기준 날짜를 오늘 대신 지정 (다중 주기 빠른 테스트)
  --dry-run                    DB 적재 없이 실행 계획만 출력 (다운로드는 실행됨)
  --manual                     down.json 의 manual_start/manual_end 날짜 범위 사용
  --diagnose                   pipeline_coverage 현재 상태 진단 출력 (DB 변경 없음)

주요 실행 예시:
  python run.py                                    # 자동 일일 파이프라인 (기본)
  python run.py --mode download_only               # 재분석 다운로드만 (DB 불필요)
  python run.py --mode load_only                   # 기존 파일 DB 적재만
  python run.py --mode forecast_only               # 예보 다운로드 + 적재만
  python run.py --init-db                          # DB 스키마 초기화만

  python run.py --manual                           # down.json 날짜 범위로 수동 백필
  python run.py --manual --mode download_only      # 수동 날짜로 다운로드만
  python run.py --dry-run                          # 실행 계획만 확인 (DB 변경 없음)
  python run.py --diagnose                         # coverage 상태 확인
  python run.py --simulate-date 2026-03-01         # 3월 1일 기준으로 파이프라인 실행
  python run.py --forecast-days 1                  # 예보 1일치만 (빠른 테스트)
"""

import sys
import argparse
import tomllib
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv    # .env 파일 로드
from loguru import logger         # 로그 출력


def setup_logging(log_dir: Path) -> None:
    """
    로그 설정 초기화

    - 콘솔: INFO 이상 출력 (색상 포함)
    - 파일: DEBUG 이상 저장 (날짜별 파일, 7일 보관)
    """
    # 로그 폴더 생성
    log_dir.mkdir(parents=True, exist_ok=True)

    # 기존 로그 핸들러 제거 (loguru 기본 핸들러 교체)
    logger.remove()

    # 콘솔 출력: INFO 이상, 색상 포함
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan> | "
               "<level>{message}</level>",
        colorize=True,
    )

    # 파일 출력: DEBUG 이상, 날짜별 파일, 7일 보관 후 삭제
    logger.add(
        log_dir / "pipeline_{time:YYYY-MM-DD}.log",
        level="DEBUG",
        rotation="1 day",     # 매일 새 파일 생성
        retention="7 days",   # 7일 지난 파일 자동 삭제
        encoding="utf-8",
    )


def main():
    """메인 실행 함수"""

    # ── 커맨드라인 인수 파서 설정 ──
    parser = argparse.ArgumentParser(
        description="선박 환경 데이터 수집/적재 파이프라인",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
실행 예시:
  python run.py                                   자동 일일 파이프라인 (기본값)
  python run.py --mode download_only              재분석 다운로드만 (DB 불필요)
  python run.py --mode load_only                  기존 파일 DB 적재만
  python run.py --mode forecast_only              예보 다운로드 + 적재만
  python run.py --init-db                         DB 스키마만 생성
  python run.py --manual                          down.json 날짜 범위로 수동 백필
  python run.py --dry-run                         실행 계획 확인 (DB 변경 없음)
  python run.py --diagnose                        pipeline_coverage 상태 진단
  python run.py --simulate-date 2026-03-01        특정 날짜 기준으로 실행 (테스트)
  python run.py --forecast-days 1                 예보 1일치만 (빠른 테스트)
        """,
    )

    # ── 기본 실행 모드 ──
    # auto: Phase 10 기본값 (coverage 기반 자동 누락 감지)
    # 구 모드(full, forecast 등)도 하위 호환으로 지원
    parser.add_argument(
        "--mode",
        choices=[
            "auto",                    # 자동 일일 파이프라인 (Phase 10 기본값)
            "download_only",           # 재분석 다운로드만 (DB/Docker 불필요)
            "load_only",               # 재분석 기존 파일 DB 적재만
            "forecast_only",           # 예보 다운로드 + DB 적재만
            "forecast_download_only",  # 예보 다운로드만 (DB 적재 없음)
            # 하위 호환 (Phase 9 이전 모드명)
            "full",                    # → auto 와 동일
            "forecast",                # → forecast_only 와 동일
            "full_with_forecast",      # → auto 와 동일
        ],
        default="auto",               # 기본값: 자동 일일 파이프라인
        help="실행 모드 선택 (기본값: auto)",
    )

    # ── --init-db: DB 스키마만 생성 ──
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="DB 테이블 스키마만 생성하고 종료",
    )

    # ── --forecast-days: 예보 기간 오버라이드 (테스트용) ──
    parser.add_argument(
        "--forecast-days",
        type=int,
        default=None,
        metavar="N",
        help=(
            "예보 다운로드 기간 오버라이드 (일 단위, 1~10). "
            "미지정 시 settings.toml 의 forecast_days 값 사용. "
            "예: --forecast-days 1 → 내일까지만 (빠른 테스트)"
        ),
    )

    # ══════════════════════════════════════
    # Phase 10 신규 플래그
    # ══════════════════════════════════════

    # --simulate-date: 오늘 날짜를 다른 날짜로 대체 (다중 주기 빠른 테스트)
    # 실제 스케줄러는 하루 1회 실행이지만 --simulate-date 로 여러 날짜를 순서대로
    # 테스트하면 며칠치 파이프라인 로직을 수분 내에 검증 가능
    parser.add_argument(
        "--simulate-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "기준 날짜를 오늘 대신 지정 (YYYY-MM-DD 형식). "
            "coverage 기반 누락 감지 등 날짜 의존 로직 테스트에 사용. "
            "예: --simulate-date 2026-03-01"
        ),
    )

    # --dry-run: DB 적재 없이 실행 계획만 출력
    # 다운로드는 실제로 수행되지만 DB INSERT/UPDATE/DELETE 는 모두 생략
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "DB 적재 없이 실행 계획만 출력. "
            "다운로드는 실제 수행됨. 로직 검증에 활용."
        ),
    )

    # --manual: down.json 의 manual_start/manual_end 날짜 범위로 백필
    # 자동 모드(auto)에서는 coverage 기반으로 날짜를 감지하지만,
    # --manual 을 붙이면 down.json 에 명시한 날짜 범위를 사용
    parser.add_argument(
        "--manual",
        action="store_true",
        help=(
            "config/down.json 의 manual_start/manual_end 날짜 범위 사용. "
            "ERA5 특성상 manual_end 는 오늘 기준 -7일 이전 권장."
        ),
    )

    # --diagnose: pipeline_coverage 현재 상태를 표 형태로 출력 (DB 변경 없음)
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help=(
            "pipeline_coverage 테이블 현재 상태 진단 출력. "
            "DB 변경 없이 읽기 전용으로 동작."
        ),
    )

    # 인수 파싱 실행
    args = parser.parse_args()

    # ── 프로젝트 루트 경로 설정 ──
    project_root = Path(__file__).parent

    # ── 로그 설정 ──
    log_dir = project_root / "logs"
    setup_logging(log_dir)

    # ── .env 파일 로드 ──
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        logger.debug(".env 파일 로드 완료")
    else:
        logger.warning(
            ".env 파일이 없습니다. .env.example을 복사해서 .env를 만들어주세요.\n"
            "  cp .env.example .env"
        )

    # ── .env 로드 후 import (환경변수 적용 필요) ──
    from src.env_pipeline.pipeline import run_pipeline
    from src.env_pipeline.db.schema import initialize_schema

    # ── --simulate-date 파싱 ──
    today_override: date | None = None
    if args.simulate_date:
        try:
            today_override = datetime.strptime(args.simulate_date, "%Y-%m-%d").date()
            logger.info(f"시뮬레이션 날짜 설정: {today_override}")
        except ValueError:
            logger.error(
                f"--simulate-date 형식 오류: '{args.simulate_date}' "
                f"(올바른 형식: YYYY-MM-DD, 예: 2026-03-01)"
            )
            sys.exit(1)

    # ── --forecast-days 유효성 검사 ──
    if args.forecast_days is not None and not (1 <= args.forecast_days <= 10):
        logger.error("--forecast-days 값은 1~10 사이여야 합니다.")
        sys.exit(1)

    # ── 실행 분기 ──

    if args.init_db:
        # --init-db: DB 스키마만 생성하고 종료
        logger.info("DB 스키마 초기화 전용 실행")
        initialize_schema()
        logger.success("스키마 초기화 완료!")
        return

    if args.diagnose:
        # --diagnose: coverage 상태 진단 출력 (DB 변경 없음)
        from src.env_pipeline.db.connection import get_connection
        from src.env_pipeline.db.coverage import print_diagnosis

        # settings.toml 에서 lookback_days 읽기
        config_path = project_root / "config" / "settings.toml"
        with open(config_path, "rb") as f:
            config_data = tomllib.load(f)
        lookback_days = config_data.get("pipeline", {}).get("coverage_lookback_days", 30)

        today = today_override or date.today()
        logger.info(f"진단 기준 날짜: {today}")

        conn = get_connection()
        try:
            print_diagnosis(conn, today, lookback_days)
        finally:
            conn.close()
        return

    # ── 일반 파이프라인 실행 ──
    run_pipeline(
        mode=args.mode,
        today_override=today_override,
        dry_run=args.dry_run,
        manual_mode=args.manual,
        forecast_days_override=args.forecast_days,
    )


# ── 스크립트 직접 실행 시 main() 호출 ──
if __name__ == "__main__":
    main()
