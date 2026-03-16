"""
환경 데이터 파이프라인 실행 진입점
====================================

날짜 범위 설정 (재분석): config/down.json 의 start_date / end_date 수정
예보 날짜: 항상 오늘 기준 자동 계산 (입력 불필요)

사용법:
  python run.py --mode download_only          # 재분석 다운로드만 (DB 생략)
  python run.py --mode load_only              # 재분석 기존 파일 DB 적재만
  python run.py --mode full                   # 재분석 다운로드 + DB 적재
  python run.py --mode forecast               # 예보 다운로드 + DB 적재
  python run.py --mode forecast_download_only # 예보 다운로드만 (DB 생략)
  python run.py --mode full_with_forecast     # 재분석 + 예보 동시 실행
  python run.py --init-db                     # DB 테이블만 생성 (데이터 수집 X)

예보 기간 오버라이드 (테스트용):
  python run.py --mode forecast_download_only --forecast-days 1
  # settings.toml의 forecast_days(10일) 무시하고 1일치만 다운로드
  # 변수 구조 확인 후 scripts/check_forecast_vars.py 로 검사
"""

import sys           # 커맨드라인 인수 처리
import argparse      # 인수 파서 (--mode test 같은 옵션 처리)
from pathlib import Path
from dotenv import load_dotenv   # .env 파일 로드
from loguru import logger        # 로그 출력


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
  python run.py --mode download_only          재분석 다운로드만 (DB 생략)
  python run.py --mode load_only              재분석 기존 파일 DB 적재만
  python run.py --mode full                   재분석 다운로드 + DB 적재
  python run.py --mode forecast               예보 다운로드 + DB 적재
  python run.py --mode forecast_download_only 예보 다운로드만 (DB 생략)
  python run.py --mode full_with_forecast     재분석 + 예보 동시 실행
  python run.py --init-db                     DB 스키마만 생성
        """,
    )

    # --mode 옵션: 파이프라인 실행 모드 선택
    parser.add_argument(
        "--mode",
        choices=[
            "download_only",           # 재분석 다운로드만 (DB 적재 생략)
            "load_only",               # 재분석 기존 파일 DB 적재만
            "full",                    # 재분석 다운로드 + DB 적재
            "forecast",                # 예보 다운로드 + DB 적재
            "forecast_download_only",  # 예보 다운로드만 (DB 적재 생략)
            "full_with_forecast",      # 재분석 + 예보 동시 실행 + DB 적재
        ],
        default="download_only",  # 기본값: 재분석 다운로드만
        help="실행 모드 선택 (기본값: download_only)",
    )

    # --init-db 옵션: DB 스키마만 생성 (데이터 수집 없이)
    parser.add_argument(
        "--init-db",
        action="store_true",  # 플래그 옵션 (값 없이 지정만 해도 True)
        help="DB 테이블 스키마만 생성하고 종료",
    )

    # --forecast-days 옵션: settings.toml 값을 무시하고 예보 기간을 직접 지정
    # 주로 테스트 목적 — 1일치만 받아서 변수 구조 확인 등
    # 미지정 시 settings.toml의 forecast_days 값 사용 (기본 10일)
    parser.add_argument(
        "--forecast-days",
        type=int,               # 정수 값 받음
        default=None,           # 미지정 시 None → settings.toml 값 사용
        metavar="N",            # 도움말에 표시될 인수 이름
        help=(
            "예보 다운로드 기간 오버라이드 (일 단위, 1~10). "
            "미지정 시 settings.toml의 forecast_days 사용. "
            "예: --forecast-days 1 → 내일까지만 다운로드 (빠른 테스트용)"
        ),
    )

    # 인수 파싱 실행
    args = parser.parse_args()

    # ── 프로젝트 루트 경로 설정 ──
    # 이 파일(run.py)이 있는 폴더 = 프로젝트 루트
    project_root = Path(__file__).parent

    # ── 로그 설정 ──
    log_dir = project_root / "logs"
    setup_logging(log_dir)

    # ── .env 파일 로드 ──
    # DB 비밀번호, API 키 등 민감 정보를 환경변수로 로드
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        logger.debug(".env 파일 로드 완료")
    else:
        logger.warning(
            ".env 파일이 없습니다. .env.example을 복사해서 .env를 만들어주세요.\n"
            "  cp .env.example .env"
        )

    # ── 실행 분기 ──
    # 여기서 import 하는 이유: .env 로드 후에 import 해야 환경변수가 적용됨
    from src.env_pipeline.pipeline import run_pipeline
    from src.env_pipeline.db.schema import initialize_schema

    if args.init_db:
        # --init-db 플래그: DB 스키마만 생성하고 종료
        logger.info("DB 스키마 초기화 전용 실행")
        initialize_schema()
        logger.success("완료! 이제 run.py --mode test 로 데이터를 수집하세요.")
        return

    # ── --forecast-days 값 유효성 검사 ──
    if args.forecast_days is not None:
        if not (1 <= args.forecast_days <= 10):
            logger.error("--forecast-days 값은 1~10 사이여야 합니다.")
            sys.exit(1)
        logger.info(f"예보 기간 오버라이드: {args.forecast_days}일 (settings.toml 무시)")

    # 파이프라인 실행
    run_pipeline(mode=args.mode, forecast_days_override=args.forecast_days)


# ── 스크립트 직접 실행 시 main() 호출 ──
# "python run.py" 로 실행할 때만 main() 호출
# "import run" 으로 모듈로 가져올 때는 호출 안 됨
if __name__ == "__main__":
    main()
