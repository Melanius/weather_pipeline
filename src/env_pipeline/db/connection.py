"""
PostgreSQL (TimescaleDB) 데이터베이스 연결 관리 모듈
===================================================

이 모듈은 DB 연결을 한 곳에서 관리함.
다른 모듈에서는 get_connection() 함수만 호출하면 됨.
"""

import os                  # 환경변수 읽기
import psycopg2            # PostgreSQL 연결 라이브러리
from loguru import logger  # 로그 출력


def get_connection():
    """
    .env 파일의 환경변수를 읽어서 PostgreSQL 연결 객체를 반환

    Returns
    -------
    psycopg2.connection
        PostgreSQL 연결 객체. 사용 후 .close() 호출 필요.

    사용 예:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
    """
    # .env 파일에서 읽어온 환경변수로 연결 문자열 구성
    connection_params = {
        "host":     os.environ.get("DB_HOST",     "localhost"),  # DB 서버 주소
        "port":     int(os.environ.get("DB_PORT", "5432")),      # 포트 번호
        "dbname":   os.environ.get("DB_NAME",     "ship_env"),   # DB 이름
        "user":     os.environ.get("DB_USER",     "shipllm"),    # 사용자명
        "password": os.environ.get("DB_PASSWORD", ""),           # 비밀번호
    }

    try:
        # 실제 DB에 연결 시도
        conn = psycopg2.connect(**connection_params)
        logger.debug(
            f"DB 연결 성공: {connection_params['host']}:{connection_params['port']}"
            f"/{connection_params['dbname']}"
        )
        return conn

    except psycopg2.OperationalError as e:
        # 연결 실패 시 (DB가 꺼져있거나, 주소/비밀번호 틀린 경우 등)
        logger.error(f"DB 연결 실패: {e}")
        logger.error(
            "확인사항: Docker TimescaleDB가 실행 중인지 확인하세요.\n"
            "  → docker-compose up -d"
        )
        raise  # 에러를 다시 위로 전달 (프로그램이 에러를 알 수 있도록)


def test_connection() -> bool:
    """
    DB 연결이 정상인지 테스트

    Returns
    -------
    bool  →  True: 연결 정상, False: 연결 실패
    """
    try:
        conn = get_connection()

        # SELECT 1은 DB가 정상 동작하는지 확인하는 가장 간단한 쿼리
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()

        logger.info("DB 연결 테스트: 정상")
        return True

    except Exception:
        logger.error("DB 연결 테스트: 실패")
        return False
