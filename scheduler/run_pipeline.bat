@echo off
REM =====================================================
REM  llm_for_ship 환경 데이터 파이프라인 실행 배치
REM  Windows Task Scheduler에서 호출하는 진입점
REM
REM  동작:
REM    1. WSL(Ubuntu)을 통해 Python 파이프라인 실행
REM    2. 로그는 logs/pipeline_YYYY-MM-DD.log에 자동 저장
REM       (loguru 설정으로 날짜별 파일 자동 분리)
REM
REM  전제 조건:
REM    - Docker Desktop 실행 중 (TimescaleDB 컨테이너 기동)
REM    - WSL2(Ubuntu) 설치됨
REM    - /mnt/c/Users/hjlee/llm_for_ship에 프로젝트 존재
REM =====================================================

REM WSL을 통해 파이프라인 실행
REM  -e bash -c "..." : bash 셸로 명령 실행
REM  cd 후 uv run으로 가상환경 자동 활성화
wsl.exe -e bash -c "cd /mnt/c/Users/hjlee/llm_for_ship && uv run python run.py --mode auto"
