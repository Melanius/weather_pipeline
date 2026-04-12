"""
파이프라인 상태 추적 모듈 (Phase 10, Phase 14-A, Phase 17 개선)
================================================================

pipeline_coverage 테이블을 읽고 쓰는 모든 함수를 담당.

역할:
  - 날짜별·소스별 적재 상태 추적 (complete/partial/forecast_only/missing/failed 등)
  - 다운로드 상태(download_status)와 DB 적재 상태(load_status)를 분리 추적 (Phase 17)
  - 누락 날짜 자동 감지 → 백필 대상 목록 반환
  - 재분석 완료 날짜에 대한 예보 데이터 삭제 (cleanup)
  - HYCOM 롤링 윈도우 초과 날짜 → permanent_forecast 승격
  - 재시도 대상 조회 (get_retry_targets) — STEP 5 자동 재시도용 (Phase 17)
  - --diagnose 모드에서 현재 상태 표 출력

status 컬럼 값 정의 (레거시 + 현행):
  'complete'           → DB 적재 완전 완료 (정상)
  'partial'            → 일부만 적재됨 (재시도 대상)
  'forecast_only'      → 재분석 미제공 → 예보로 임시 커버
  'permanent_forecast' → HYCOM 롤링 윈도우 초과 → 예보가 영구 확정 (cleanup 제외)
  'missing'            → 어떤 데이터도 없음
  'failed'             → 다운로드/적재 시도 중 오류 발생

download_status 컬럼 값 정의 (Phase 17 신규):
  'complete' → 다운로드 완전 완료 (파일 정상)
  'failed'   → 다운로드 실패 (네트워크 오류, 서버 미제공 등)
  'skipped'  → 해당 소스 다운로드 불필요
  NULL       → 아직 시도 전 (또는 레거시 레코드)

load_status 컬럼 값 정의 (Phase 17 신규):
  'complete' → DB 적재 완전 완료
  'partial'  → 일부만 적재 (예: wind 성공 + wave 실패)
  'failed'   → 적재 시도 자체가 실패
  'skipped'  → 다운로드 실패로 인해 적재 미시도
  NULL       → 아직 시도 전 (또는 레거시 레코드)

Phase 14-A 변경:
  - 완료 판정 기준을 '행 수 >= expected × threshold' 에서 '예외 없음' 으로 변경.
  - EXPECTED_ROWS 상수 제거. expected_rows 컬럼은 NULL로 유지 (DB 컬럼 자체는 보존).

Phase 17 변경:
  - update_coverage()에 download_status / load_status 파라미터 추가
  - get_retry_targets() 신규 추가 — STEP 5 자동 재시도 대상 분류
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg2
from loguru import logger


# ─────────────────────────────────────────────────────
# 상태 상수 (오타 방지용)
# ─────────────────────────────────────────────────────

# ── status 컬럼 (기존, 전체 상태 요약) ──
STATUS_COMPLETE            = "complete"           # 적재 완전 완료
STATUS_PARTIAL             = "partial"            # 부분 적재 (재시도 필요)
STATUS_FORECAST_ONLY       = "forecast_only"      # 예보로 임시 커버
STATUS_PERMANENT_FORECAST  = "permanent_forecast" # 예보가 영구 확정 (HYCOM 윈도우 초과)
STATUS_MISSING             = "missing"            # 데이터 없음
STATUS_FAILED              = "failed"             # 오류 발생

# ── download_status 컬럼 (Phase 17: 다운로드 단계 상태) ──
DL_COMPLETE = "complete"   # 파일 다운로드 완료 및 정상 확인
DL_FAILED   = "failed"     # 다운로드 실패 (네트워크/서버 오류)
DL_SKIPPED  = "skipped"    # 해당 소스 다운로드 불필요 (예: 예보 소스의 재분석)

# ── load_status 컬럼 (Phase 17: DB 적재 단계 상태) ──
LOAD_COMPLETE = "complete"  # DB 적재 완전 완료
LOAD_PARTIAL  = "partial"   # 부분 적재 (예: wind 성공 + wave 실패)
LOAD_FAILED   = "failed"    # 적재 시도 자체 실패 (연결 오류 등)
LOAD_SKIPPED  = "skipped"   # 다운로드 실패로 인해 적재 미시도

# ─────────────────────────────────────────────────────
# 소스 상수
# ─────────────────────────────────────────────────────

SOURCE_ECMWF_REANALYSIS = "ecmwf_reanalysis"   # ERA5/ERA5T 재분석 (바람+파랑)
SOURCE_HYCOM_CURRENT    = "hycom_current"       # HYCOM 해류 분석
SOURCE_ECMWF_FORECAST   = "ecmwf_forecast"      # ECMWF 바람 예보
SOURCE_NOAA_FORECAST    = "noaa_forecast"       # NOAA WW3 파랑 예보
SOURCE_HYCOM_FORECAST   = "hycom_forecast"      # HYCOM 해류 예보

# 재분석 소스 목록 (예보 cleanup 판단 기준)
REANALYSIS_SOURCES = [SOURCE_ECMWF_REANALYSIS, SOURCE_HYCOM_CURRENT]

# 예보 소스 목록 (cleanup 대상)
FORECAST_SOURCES = [SOURCE_ECMWF_FORECAST, SOURCE_NOAA_FORECAST, SOURCE_HYCOM_FORECAST]

# 예보 소스 → 실제 DB 테이블명 매핑 (cleanup 시 DELETE 대상)
FORECAST_SOURCE_TO_TABLE = {
    SOURCE_ECMWF_FORECAST: "env_ecmwf_forecast",
    SOURCE_NOAA_FORECAST:  "env_noaa_forecast",
    SOURCE_HYCOM_FORECAST: "env_hycom_forecast",
}

# ─────────────────────────────────────────────────────
# 상태 갱신 함수
# ─────────────────────────────────────────────────────

def update_coverage(
    conn,
    target_date: date,
    source: str,
    status: str,
    row_count: int | None = None,
    data_type: str | None = None,
    notes: str | None = None,
    download_status: str | None = None,
    load_status: str | None = None,
) -> None:
    """
    pipeline_coverage 테이블에 상태를 기록 (INSERT 또는 UPDATE)

    이미 해당 (date, source) 레코드가 있으면 UPDATE,
    없으면 INSERT (UPSERT 방식).

    Parameters
    ----------
    conn            : psycopg2 연결 객체
    target_date     : 기록할 날짜
    source          : 데이터 소스 (SOURCE_* 상수 사용)
    status          : 전체 상태 요약 (STATUS_* 상수 사용)
    row_count       : 실제 적재된 행 수 (None이면 기존 값 유지)
    data_type       : 'era5' 또는 'era5t' (None이면 기존 값 유지)
    notes           : 부가 메모 (None이면 기존 값 유지)
    download_status : 다운로드 단계 상태 (DL_* 상수, None이면 기존 값 유지)
    load_status     : DB 적재 단계 상태 (LOAD_* 상수, None이면 기존 값 유지)
    """

    cursor = conn.cursor()
    # INSERT ON CONFLICT DO UPDATE 방식
    # expected_rows 는 NULL로 저장 (Phase 14-A: 행 수 비교 판정 제거)
    # download_status / load_status: COALESCE → None이면 기존 값 유지
    cursor.execute("""
        INSERT INTO pipeline_coverage
            (date, source, status, data_type, row_count, expected_rows,
             loaded_at, notes, download_status, load_status)
        VALUES
            (%s, %s, %s, %s, %s, NULL,
             NOW() AT TIME ZONE 'UTC', %s, %s, %s)
        ON CONFLICT (date, source) DO UPDATE SET
            status          = EXCLUDED.status,
            data_type       = COALESCE(EXCLUDED.data_type,        pipeline_coverage.data_type),
            row_count       = COALESCE(EXCLUDED.row_count,        pipeline_coverage.row_count),
            loaded_at       = EXCLUDED.loaded_at,
            notes           = COALESCE(EXCLUDED.notes,            pipeline_coverage.notes),
            download_status = COALESCE(EXCLUDED.download_status,  pipeline_coverage.download_status),
            load_status     = COALESCE(EXCLUDED.load_status,      pipeline_coverage.load_status)
    """, (
        target_date,
        source,
        status,
        data_type,
        row_count,
        notes,
        download_status,
        load_status,
    ))
    # 호출자가 conn.commit() 책임 (트랜잭션 제어권 유지)


# ─────────────────────────────────────────────────────
# STEP 5 자동 재시도 대상 조회 (Phase 17 신규)
# ─────────────────────────────────────────────────────

def get_retry_targets(
    conn,
    today: date,
    lookback_days: int,
    reanalysis_start_date: date | None = None,
) -> list[dict]:
    """
    자동 재시도(STEP 5)가 필요한 항목 목록 반환

    각 항목에 대해 다운로드부터 다시 해야 하는지('download'),
    적재만 다시 하면 되는지('load_only')를 분류하여 반환.

    분류 기준:
    ┌────────────────────────┬───────────────────────────────────────────────┐
    │ retry_type             │ 조건                                           │
    ├────────────────────────┼───────────────────────────────────────────────┤
    │ 'download'             │ download_status IS NULL or 'failed'            │
    │                        │ → 다운로드부터 다시 시작 (적재도 포함)            │
    ├────────────────────────┼───────────────────────────────────────────────┤
    │ 'load_only'            │ download_status = 'complete'                   │
    │                        │ AND load_status IN (NULL, 'partial', 'failed') │
    │                        │ → 다운로드 완료, 적재만 재시도                    │
    └────────────────────────┴───────────────────────────────────────────────┘

    제외 대상:
      - status IN ('complete', 'permanent_forecast'): 이미 완료
      - download_status = 'skipped' or load_status = 'skipped': 의도적 건너뜀
      - 재분석 소스의 경우 lookback 범위 초과 날짜

    레거시 레코드 처리:
      - download_status IS NULL + load_status IS NULL + status IN ('partial', 'failed', 'missing')
        → 'download' 타입으로 분류 (다운로드부터 다시 시도)

    Parameters
    ----------
    conn                  : psycopg2 연결 객체
    today                 : 기준 날짜
    lookback_days         : 재시도 대상으로 볼 최대 과거 범위 (일)
    reanalysis_start_date : 재분석 수집 최초 시작일 하한선 (None이면 미적용)

    Returns
    -------
    list[dict]: 재시도 대상 목록 (날짜순 정렬)
        각 dict:
          {
            'date':             date,
            'source':           str,        # SOURCE_* 상수 값
            'retry_type':       str,        # 'download' | 'load_only'
            'status':           str,        # 현재 status 컬럼 값
            'download_status':  str|None,   # 현재 download_status 값
            'load_status':      str|None,   # 현재 load_status 값
          }
    """
    # 조회 범위 하한선 결정
    rolling_start = today - timedelta(days=lookback_days)
    range_start = max(rolling_start, reanalysis_start_date) \
        if reanalysis_start_date else rolling_start

    cursor = conn.cursor()

    # 완료/영구 예보/건너뜀을 제외한 모든 레코드 조회
    cursor.execute("""
        SELECT date, source, status, download_status, load_status
        FROM pipeline_coverage
        WHERE date >= %s
          AND date <= %s
          AND status NOT IN (%s, %s)
          AND COALESCE(download_status, '') != %s
          AND COALESCE(load_status, '')     != %s
        ORDER BY date, source
    """, (
        range_start,
        today,
        STATUS_COMPLETE,
        STATUS_PERMANENT_FORECAST,
        DL_SKIPPED,
        LOAD_SKIPPED,
    ))
    rows = cursor.fetchall()

    retry_targets = []
    for d, src, st, dl_st, ld_st in rows:

        # 분류 로직
        if dl_st == DL_COMPLETE:
            # 다운로드 완료 → 적재 상태 확인
            if ld_st in (None, LOAD_PARTIAL, LOAD_FAILED):
                retry_type = "load_only"
            else:
                # load_status = 'complete' 이지만 status != 'complete' → 불일치, 건너뜀
                continue
        else:
            # download_status가 NULL 또는 'failed' → 다운로드부터
            retry_type = "download"

        retry_targets.append({
            "date":            d,
            "source":          src,
            "retry_type":      retry_type,
            "status":          st,
            "download_status": dl_st,
            "load_status":     ld_st,
        })

    # 소스별 집계 로그
    from collections import Counter
    cnt = Counter(f"{t['source']}:{t['retry_type']}" for t in retry_targets)
    if retry_targets:
        logger.info(
            f"STEP 5 재시도 대상: 총 {len(retry_targets)}건 "
            f"(범위: {range_start} ~ {today})"
        )
        for key, n in sorted(cnt.items()):
            logger.info(f"  {key}: {n}건")
    else:
        logger.info("STEP 5 재시도 대상: 없음 (모두 완료 상태)")

    return retry_targets


# ─────────────────────────────────────────────────────
# 누락 날짜 조회 함수
# ─────────────────────────────────────────────────────

def get_backfill_dates(
    conn,
    today: date,
    lookback_days: int,
    era5_delay_days: int,
    reanalysis_start_date: date | None = None,
) -> dict[str, list[date]]:
    """
    백필이 필요한 날짜 목록을 소스별로 반환

    스캔 범위:
      - ECMWF 재분석: [range_start, today - era5_delay_days]
        (era5_delay_days 이내는 아직 API 미제공이므로 시도하지 않음)
      - HYCOM 분석: 동일 범위 (HYCOM은 보통 당일~1일 지연 제공)

    range_start 결정 규칙:
      max(today - lookback_days, reanalysis_start_date)
      → 두 날짜 중 더 최근 날짜 사용 (하한선 적용)
      → reanalysis_start_date 이전 날짜는 절대 수집하지 않음

    'complete' 또는 'permanent_forecast' 상태는 백필 불필요로 제외.

    Parameters
    ----------
    conn                   : psycopg2 연결 객체
    today                  : 기준 날짜 (--simulate-date 지원)
    lookback_days          : 스캔할 과거 기간 (일)
    era5_delay_days        : ERA5 API 안전 지연 일수
    reanalysis_start_date  : 재분석 수집 최초 시작일 (고정 하한선, None이면 미적용)

    Returns
    -------
    dict: {source: [date, ...]} — 소스별 백필 필요 날짜 목록
    """
    # 스캔 범위 계산
    rolling_start = today - timedelta(days=lookback_days)

    # reanalysis_start_date가 설정된 경우: 두 날짜 중 더 최근 날짜를 하한선으로 사용
    # 예) rolling_start=3/6, start_date=3/29 → max → 3/29 (3/29 이전은 수집 안 함)
    # 예) rolling_start=5/2, start_date=3/29 → max → 5/2  (롤링이 더 최근이면 그대로)
    if reanalysis_start_date is not None:
        range_start = max(rolling_start, reanalysis_start_date)
    else:
        range_start = rolling_start

    range_end   = today - timedelta(days=era5_delay_days)

    if range_start > range_end:
        # 설정 오류: lookback < delay 이면 범위가 없음
        return {SOURCE_ECMWF_REANALYSIS: [], SOURCE_HYCOM_CURRENT: []}

    # 스캔 범위 내 모든 날짜 목록
    all_dates = [
        range_start + timedelta(days=i)
        for i in range((range_end - range_start).days + 1)
    ]

    cursor = conn.cursor()
    result: dict[str, list[date]] = {}

    for source in REANALYSIS_SOURCES:
        # pipeline_coverage에서 이미 완료된 날짜 조회
        cursor.execute("""
            SELECT date FROM pipeline_coverage
            WHERE source = %s
              AND status IN (%s, %s)
              AND date >= %s
              AND date <= %s
        """, (
            source,
            STATUS_COMPLETE,
            STATUS_PERMANENT_FORECAST,  # permanent_forecast도 재시도 불필요
            range_start,
            range_end,
        ))
        done_dates = {row[0] for row in cursor.fetchall()}

        # 완료되지 않은 날짜만 반환
        result[source] = [d for d in all_dates if d not in done_dates]

    logger.info(
        f"백필 대상 날짜: "
        f"ECMWF {len(result[SOURCE_ECMWF_REANALYSIS])}일 / "
        f"HYCOM {len(result[SOURCE_HYCOM_CURRENT])}일 "
        f"(범위: {range_start} ~ {range_end})"
    )
    return result


# ─────────────────────────────────────────────────────
# 예보 데이터 cleanup 함수
# ─────────────────────────────────────────────────────

def cleanup_superseded_forecasts(
    conn,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    재분석이 'complete'인 날짜의 예보 데이터를 삭제 (예보→재분석 교체)

    동작 규칙:
      - ecmwf_reanalysis 'complete' → ecmwf_forecast, noaa_forecast 삭제
      - hycom_current 'complete'    → hycom_forecast 삭제
      - 'permanent_forecast' 날짜는 삭제 제외 (영구 보존)
      - 재분석 'partial'/'failed' 날짜는 삭제 보류 (예보 데이터 유지)
      - dry_run=True 이면 실제 삭제 없이 삭제 예정 내용만 로그 출력

    Parameters
    ----------
    conn    : psycopg2 연결 객체
    dry_run : True이면 실행하지 않고 계획만 출력

    Returns
    -------
    dict: {"ecmwf_forecast": N, "noaa_forecast": N, "hycom_forecast": N}
        각 테이블에서 삭제된 행 수 (dry_run이면 0)
    """
    cursor = conn.cursor()
    deleted = {src: 0 for src in FORECAST_SOURCES}

    # ── ECMWF 재분석 complete 날짜 조회 ──
    cursor.execute("""
        SELECT date FROM pipeline_coverage
        WHERE source = %s AND status = %s
    """, (SOURCE_ECMWF_REANALYSIS, STATUS_COMPLETE))
    ecmwf_complete = {row[0] for row in cursor.fetchall()}

    # ── HYCOM 분석 complete 날짜 조회 ──
    cursor.execute("""
        SELECT date FROM pipeline_coverage
        WHERE source = %s AND status = %s
    """, (SOURCE_HYCOM_CURRENT, STATUS_COMPLETE))
    hycom_complete = {row[0] for row in cursor.fetchall()}

    # ── permanent_forecast 날짜 조회 (cleanup 제외 대상) ──
    cursor.execute("""
        SELECT date FROM pipeline_coverage
        WHERE status = %s
    """, (STATUS_PERMANENT_FORECAST,))
    permanent_dates = {row[0] for row in cursor.fetchall()}

    # ── ECMWF/NOAA 예보 cleanup ──
    for d in sorted(ecmwf_complete):
        if d in permanent_dates:
            continue  # permanent_forecast 날짜는 건너뜀

        for src in [SOURCE_ECMWF_FORECAST, SOURCE_NOAA_FORECAST]:
            table = FORECAST_SOURCE_TO_TABLE[src]
            if dry_run:
                # 실제 삭제 없이 삭제 예정 행 수만 확인
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE date(datetime) = %s", (d,)
                )
                count = cursor.fetchone()[0]
                if count > 0:
                    logger.info(f"[DRY-RUN] {d}: {table} 에서 {count:,}행 삭제 예정")
            else:
                cursor.execute(
                    f"DELETE FROM {table} WHERE date(datetime) = %s", (d,)
                )
                rows_deleted = cursor.rowcount
                deleted[src] += rows_deleted
                if rows_deleted > 0:
                    logger.info(f"[CLEANUP] {d}: {table} {rows_deleted:,}행 삭제")

    # ── HYCOM 예보 cleanup (HYCOM 재분석도 complete인 날짜만) ──
    for d in sorted(hycom_complete):
        if d in permanent_dates:
            continue

        table = FORECAST_SOURCE_TO_TABLE[SOURCE_HYCOM_FORECAST]
        if dry_run:
            cursor.execute(
                f"SELECT COUNT(*) FROM {table} WHERE date(datetime) = %s", (d,)
            )
            count = cursor.fetchone()[0]
            if count > 0:
                logger.info(f"[DRY-RUN] {d}: {table} 에서 {count:,}행 삭제 예정")
        else:
            cursor.execute(
                f"DELETE FROM {table} WHERE date(datetime) = %s", (d,)
            )
            rows_deleted = cursor.rowcount
            deleted[SOURCE_HYCOM_FORECAST] += rows_deleted
            if rows_deleted > 0:
                logger.info(f"[CLEANUP] {d}: {table} {rows_deleted:,}행 삭제")

    if not dry_run:
        conn.commit()

    total = sum(deleted.values())
    if total > 0:
        logger.success(f"예보 cleanup 완료 | 총 {total:,}행 삭제")
    else:
        logger.info("예보 cleanup: 삭제 대상 없음")

    return deleted


# ─────────────────────────────────────────────────────
# HYCOM 영구 소실 감지 및 permanent_forecast 승격
# ─────────────────────────────────────────────────────

def check_and_promote_hycom_permanent(
    conn,
    today: date,
    hycom_window_days: int,
) -> list[date]:
    """
    HYCOM 롤링 윈도우를 초과한 missing/failed 날짜를 permanent_forecast로 승격

    HYCOM 분석 데이터는 약 D-10일 ~ D+5일 롤링 윈도우만 보유.
    이 윈도우를 넘어선 날짜는 HYCOM 분석 데이터를 영구적으로 얻을 수 없음.
    → hycom_forecast 데이터가 있다면 그것을 영구 보존 (cleanup 제외)
    → 경고 로그 출력

    Parameters
    ----------
    conn              : psycopg2 연결 객체
    today             : 기준 날짜
    hycom_window_days : HYCOM 롤링 윈도우 기간 (일)

    Returns
    -------
    list[date]: 이번에 permanent_forecast로 승격된 날짜 목록
    """
    # 윈도우 초과 기준일: 이 날짜보다 이전 날짜는 HYCOM 분석 데이터 영구 소실
    window_cutoff = today - timedelta(days=hycom_window_days)

    cursor = conn.cursor()

    # 윈도우 초과 날짜 중 hycom_current가 missing/failed인 날짜 조회
    cursor.execute("""
        SELECT date FROM pipeline_coverage
        WHERE source = %s
          AND status IN (%s, %s)
          AND date < %s
    """, (
        SOURCE_HYCOM_CURRENT,
        STATUS_MISSING,
        STATUS_FAILED,
        window_cutoff,
    ))
    expired_dates = [row[0] for row in cursor.fetchall()]

    if not expired_dates:
        return []

    # permanent_forecast로 승격
    for d in expired_dates:
        notes = f"HYCOM 롤링 윈도우 {hycom_window_days}일 초과 (기준: {window_cutoff}). HYCOM 분석 데이터 영구 소실."
        cursor.execute("""
            INSERT INTO pipeline_coverage (date, source, status, loaded_at, notes)
            VALUES (%s, %s, %s, NOW() AT TIME ZONE 'UTC', %s)
            ON CONFLICT (date, source) DO UPDATE SET
                status    = EXCLUDED.status,
                loaded_at = EXCLUDED.loaded_at,
                notes     = EXCLUDED.notes
        """, (d, SOURCE_HYCOM_CURRENT, STATUS_PERMANENT_FORECAST, notes))

        logger.warning(
            f"⚠️ HYCOM 영구 소실: {d} — "
            f"롤링 윈도우({hycom_window_days}일) 초과. "
            f"해당 날짜의 hycom_forecast 데이터를 영구 보존."
        )

    conn.commit()
    return expired_dates


# ─────────────────────────────────────────────────────
# 장기 누락 경고
# ─────────────────────────────────────────────────────

def alert_long_missing(
    conn,
    today: date,
    threshold_days: int,
) -> None:
    """
    N일 이상 missing/failed 상태가 지속된 날짜에 대해 경고 로그 출력

    Parameters
    ----------
    conn           : psycopg2 연결 객체
    today          : 기준 날짜
    threshold_days : 이 일수 이상 missing이면 경고
    """
    cutoff = today - timedelta(days=threshold_days)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT date, source, status, loaded_at
        FROM pipeline_coverage
        WHERE status IN (%s, %s)
          AND date <= %s
        ORDER BY date, source
    """, (STATUS_MISSING, STATUS_FAILED, cutoff))
    rows = cursor.fetchall()

    if not rows:
        return

    logger.warning(
        f"⚠️ 장기 누락 감지: {threshold_days}일 이상 missing/failed 상태 날짜 {len(rows)}건"
    )
    for d, src, st, loaded_at in rows:
        logger.warning(f"   {d} [{src}] status={st}")


# ─────────────────────────────────────────────────────
# 진단 출력 함수 (--diagnose 옵션)
# ─────────────────────────────────────────────────────

def print_diagnosis(
    conn,
    today: date,
    lookback_days: int,
) -> None:
    """
    pipeline_coverage 테이블 현재 상태를 표 형태로 출력 (--diagnose 옵션)

    DB 변경 없이 읽기 전용으로 동작.

    Parameters
    ----------
    conn          : psycopg2 연결 객체
    today         : 기준 날짜
    lookback_days : 표시할 과거 기간 (일)
    """
    range_start = today - timedelta(days=lookback_days)
    cursor = conn.cursor()

    # 소스별 상태 집계
    cursor.execute("""
        SELECT source, status, COUNT(*) AS cnt
        FROM pipeline_coverage
        WHERE date >= %s AND date <= %s
        GROUP BY source, status
        ORDER BY source, status
    """, (range_start, today))
    summary = cursor.fetchall()

    # 상태별 상세 날짜 목록 (missing/partial/failed/forecast_only)
    cursor.execute("""
        SELECT date, source, status, row_count, data_type, notes
        FROM pipeline_coverage
        WHERE date >= %s AND date <= %s
          AND status NOT IN (%s)
        ORDER BY date DESC, source
        LIMIT 50
    """, (range_start, today, STATUS_COMPLETE))
    details = cursor.fetchall()

    logger.info("=" * 60)
    logger.info(f"pipeline_coverage 진단 (기준: {today}, 조회 범위: {lookback_days}일)")
    logger.info("=" * 60)

    # 집계 출력
    logger.info("[상태 집계]")
    current_src = None
    for src, st, cnt in summary:
        if src != current_src:
            logger.info(f"  {src}:")
            current_src = src
        logger.info(f"    {st}: {cnt}일")

    # 미완료 날짜 상세 출력
    if details:
        logger.info(f"\n[주의 필요 날짜 (최근 {len(details)}건)]")
        for d, src, st, row_count, data_type, notes in details:
            row_info = f"{row_count:,}행" if row_count else "행수 미기록"
            notes_info = f" | {notes}" if notes else ""
            logger.info(f"  {d} [{src}] {st} | {row_info}{notes_info}")
    else:
        logger.info("\n[주의 필요 날짜: 없음 — 모든 날짜 complete]")

    logger.info("=" * 60)
