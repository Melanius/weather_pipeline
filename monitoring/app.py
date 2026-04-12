"""
llm_for_ship 파이프라인 모니터링 대시보드
=========================================

실행 방법:
    uv run streamlit run monitoring/app.py

접속 주소:
    http://localhost:8501
"""

import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# 경로 및 환경 설정
# ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent   # llm_for_ship/ 루트
LOG_DIR = PROJECT_ROOT / "logs"               # 로그 파일 디렉토리

load_dotenv(PROJECT_ROOT / ".env")            # DB 비밀번호 등 환경변수 로드

# 예보 테이블별 기대 커버리지 (일)
FORECAST_EXPECTED_DAYS = {
    "env_ecmwf_forecast": {"label": "Wind 예보 (ECMWF)",    "days": 10},
    "env_hycom_forecast": {"label": "Current 예보 (HYCOM)", "days": 5},
    "env_noaa_forecast":  {"label": "Wave 예보 (NOAA)",     "days": 5},
}

# pipeline_coverage source → 3개 통합 컬럼명 매핑
# 재분석/예보 구분 없이 Wind / Wave / Current 3개 컬럼으로 통합
SOURCE_TO_COL = {
    "ecmwf_reanalysis": ["Wind",    "Wave"],   # ECMWF는 Wind+Wave 통합 테이블
    "hycom_current":    ["Current"],
    "ecmwf_forecast":   ["Wind"],
    "noaa_forecast":    ["Wave"],
    "hycom_forecast":   ["Current"],
}

# 예보 소스 컬럼 매핑 (미래 날짜 달력용)
FORECAST_COL_MAP = {
    "Wind":    "env_ecmwf_forecast",
    "Wave":    "env_noaa_forecast",
    "Current": "env_hycom_forecast",
}

# 컬럼 표시 순서
COVERAGE_COLUMN_ORDER = ["Wind", "Wave", "Current"]

# 상태별 이모지 및 레이블
STATUS_EMOJI = {
    "complete":           "✅",
    "partial":            "⚠️",
    "forecast":           "🔵",
    "permanent_forecast": "🟣",
    "missing":            "❌",
    "failed":             "🔴",
}

# pipeline_coverage DB에 저장된 구 상태명 → 신 상태명 변환
STATUS_NORMALIZE = {
    "forecast_only": "forecast",
}

# download_status / load_status 조합 → 달력 셀 이모지 (Phase 17)
# download_status: complete / failed / skipped / None(레거시)
# load_status    : complete / partial / failed / skipped / None(레거시)
DL_LOAD_EMOJI = {
    ("complete", "complete"): "✅",    # 다운+적재 모두 완료
    ("complete", "partial"):  "⚠️",    # 다운 완료, 적재 부분만
    ("complete", "failed"):   "📥🔴",  # 다운 완료, 적재 실패
    ("complete", "skipped"):  "📥⏭️", # 다운 완료, 적재 건너뜀
    ("complete", None):       "📥",    # 다운 완료, 적재 미시도
    ("failed",   None):       "🔴",    # 다운 실패
    ("failed",   "skipped"):  "🔴",    # 다운 실패 → 적재 건너뜀
    ("skipped",  "skipped"):  "⏭️",   # 둘 다 건너뜀
}

# 로그 레벨별 배경색 (pandas Styler용)
LOG_LEVEL_COLORS = {
    "SUCCESS": "#d4edda",
    "ERROR":   "#f8d7da",
    "WARNING": "#fff3cd",
    "DEBUG":   "#f8f9fa",
    "INFO":    "#ffffff",
}


# ─────────────────────────────────────────────
# DB 헬퍼
# ─────────────────────────────────────────────

def _get_conn():
    """psycopg2 연결 반환. 실패 시 None."""
    try:
        return psycopg2.connect(
            host=os.environ.get("DB_HOST",     "localhost"),
            port=int(os.environ.get("DB_PORT", "5432")),
            dbname=os.environ.get("DB_NAME",   "ship_env"),
            user=os.environ.get("DB_USER",     "shipllm"),
            password=os.environ.get("DB_PASSWORD", ""),
        )
    except Exception:
        return None


def _fetch_df(sql: str, params=None) -> pd.DataFrame:
    """SQL 실행 후 DataFrame 반환. 연결/실행 오류 시 빈 DataFrame."""
    conn = _get_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def get_table_stats() -> pd.DataFrame:
    """5개 데이터 테이블의 행수 + datetime 범위 조회"""
    tables = [
        ("env_ecmwf_reanalysis", "재분석", "Wind + Wave (ECMWF ERA5)"),
        ("env_hycom_current",    "재분석", "Current (HYCOM 분석)"),
        ("env_ecmwf_forecast",   "예보",   "Wind (ECMWF Open Data)"),
        ("env_hycom_forecast",   "예보",   "Current (HYCOM FMRC)"),
        ("env_noaa_forecast",    "예보",   "Wave (NOAA WaveWatch III)"),
    ]
    conn = _get_conn()
    if conn is None:
        return pd.DataFrame()

    rows = []
    try:
        for table, category, label in tables:
            cur = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*), MIN(datetime), MAX(datetime) FROM {table}"
            )
            count, dt_min, dt_max = cur.fetchone()
            cur.close()
            rows.append({
                "구분":      category,
                "데이터":    label,
                "행수":      f"{(count or 0):,}",
                "최초 날짜": str(dt_min.date()) if dt_min else "-",
                "최신 날짜": str(dt_max.date()) if dt_max else "-",
            })
    finally:
        conn.close()

    return pd.DataFrame(rows)


def get_forecast_horizon() -> dict:
    """예보 테이블별 실제 커버 일수 계산 (max(datetime) - today)"""
    today = date.today()
    result = {}
    conn = _get_conn()
    if conn is None:
        return {}

    try:
        for table, info in FORECAST_EXPECTED_DAYS.items():
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(datetime) FROM {table}")
            row = cur.fetchone()
            cur.close()
            max_dt = row[0] if row else None

            if max_dt:
                if hasattr(max_dt, "tzinfo") and max_dt.tzinfo:
                    max_date = max_dt.astimezone(timezone.utc).date()
                else:
                    max_date = max_dt.date() if hasattr(max_dt, "date") else max_dt
                actual_days = max(0, (max_date - today).days)
            else:
                actual_days = 0
                max_date = None

            expected = info["days"]
            ok = actual_days >= expected - 1   # 1일 오차 허용

            result[table] = {
                "label":    info["label"],
                "actual":   actual_days,
                "expected": expected,
                "max_date": str(max_date) if max_date else "-",
                "ok":       ok,
            }
    finally:
        conn.close()

    return result


def get_coverage(days: int = 30) -> pd.DataFrame:
    """pipeline_coverage 최근 N일 조회 (Phase 17: download_status / load_status 포함)"""
    start = date.today() - timedelta(days=days - 1)
    return _fetch_df(
        """
        SELECT date, source, status, row_count, loaded_at,
               download_status, load_status
        FROM   pipeline_coverage
        WHERE  date >= %s
        ORDER  BY date DESC, source
        """,
        (start,),
    )


def get_forecast_dates() -> dict[str, set]:
    """예보 테이블별 실제 존재하는 날짜 집합 반환 (미래 날짜 달력용)
    반환: {"Wind": {date, ...}, "Wave": {date, ...}, "Current": {date, ...}}
    """
    result = {}
    conn = _get_conn()
    if conn is None:
        return {col: set() for col in FORECAST_COL_MAP}
    try:
        for col_name, table in FORECAST_COL_MAP.items():
            cur = conn.cursor()
            # 오늘 이후 날짜만 조회 (미래 행만 필요)
            cur.execute(
                f"SELECT DISTINCT DATE(datetime AT TIME ZONE 'UTC') FROM {table} "
                f"WHERE datetime > %s",
                (date.today(),),
            )
            result[col_name] = {row[0] for row in cur.fetchall()}
            cur.close()
    finally:
        conn.close()
    return result


def get_missing_count() -> int:
    """pipeline_coverage에서 최근 14일 missing/failed 건수 반환"""
    df = _fetch_df(
        """
        SELECT COUNT(*)
        FROM   pipeline_coverage
        WHERE  date >= %s
          AND  status IN ('missing', 'failed')
        """,
        (date.today() - timedelta(days=13),),
    )
    if df.empty:
        return 0
    return int(df.iloc[0, 0])


# ─────────────────────────────────────────────
# 로그 파서
# ─────────────────────────────────────────────

# 로그 한 줄 형식:
#   2026-04-05 10:22:27.428 | SUCCESS  | module:func:line - 메시지
_LOG_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"   # 타임스탬프
    r"\.\d+"                                      # 밀리초 (버림)
    r"\s*\|\s*(\w+)\s*\|"                         # 로그 레벨
    r"[^|]+-\s*(.*)"                              # 메시지
)


def parse_log_file(log_path: Path, n_lines: int = 150) -> list[dict]:
    """로그 파일 마지막 n_lines 줄 파싱 → dict 리스트"""
    if not log_path.exists():
        return []
    text = log_path.read_text(encoding="utf-8", errors="ignore")
    parsed = []
    for line in text.splitlines()[-n_lines:]:
        m = _LOG_RE.match(line)
        if m:
            parsed.append({
                "시각":   m.group(1),
                "레벨":   m.group(2).strip(),
                "메시지": m.group(3).strip(),
            })
    return parsed


STUCK_THRESHOLD_MIN = 15   # 마지막 로그로부터 N분 이상 경과 시 '응답 없음' 판정


def get_last_run_info() -> dict:
    """오늘 로그 파일에서 마지막 파이프라인 실행 정보 추출.

    반환 키:
        status        : success | running | stuck | dry_run | no_run | no_log
        time          : 실행 시작 시각 문자열
        mode          : 실행 모드
        last_log_time : 마지막 로그 시각 문자열
        last_log_msg  : 마지막 로그 메시지 (running/stuck 상태 설명용)
        last_log_age  : 마지막 로그로부터 경과 분 (int)
    """
    _empty = {
        "status": "no_log", "time": "-", "mode": "-",
        "last_log_time": "-", "last_log_msg": "-", "last_log_age": 0,
    }

    today_str = date.today().strftime("%Y-%m-%d")
    log_path = LOG_DIR / f"pipeline_{today_str}.log"
    if not log_path.exists():
        return _empty

    content = log_path.read_text(encoding="utf-8", errors="ignore")

    # 오늘 로그에서 마지막 파이프라인 시작 위치 탐색
    starts = list(re.finditer(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
        r"파이프라인 시작 \| 모드: (\S+) \| dry_run: (\S+)",
        content,
    ))
    if not starts:
        return {**_empty, "status": "no_run"}

    last_start = starts[-1]
    run_time = last_start.group(1)
    mode     = last_start.group(2)
    dry_run  = last_start.group(3) == "True"

    # 마지막 시작 이후 로그만 검사
    after_start = content[last_start.start():]

    # 완료 여부
    completed = bool(re.search(r"파이프라인 완료", after_start))

    # 마지막 로그 줄 파싱 (타임스탬프 + 메시지)
    last_log_time = "-"
    last_log_msg  = "-"
    last_log_age  = 0
    for line in reversed(after_start.splitlines()):
        m = _LOG_RE.match(line)
        if m:
            last_log_time = m.group(1)
            last_log_msg  = m.group(3).strip()
            # 경과 시간 계산
            try:
                log_dt = datetime.strptime(last_log_time, "%Y-%m-%d %H:%M:%S")
                last_log_age = int((datetime.now() - log_dt).total_seconds() / 60)
            except ValueError:
                pass
            break

    # 상태 판정
    if dry_run:
        status = "dry_run"
    elif completed:
        status = "success"
    elif last_log_age >= STUCK_THRESHOLD_MIN:
        status = "stuck"
    else:
        status = "running"

    return {
        "status":        status,
        "time":          run_time,
        "mode":          mode,
        "last_log_time": last_log_time,
        "last_log_msg":  last_log_msg,
        "last_log_age":  last_log_age,
    }


# ─────────────────────────────────────────────
# Styler 헬퍼
# ─────────────────────────────────────────────

def style_coverage_pivot(pivot: pd.DataFrame) -> pd.DataFrame:
    """커버리지 피벗 테이블: 상태값 → 이모지로 변환
    - 단일 상태값: STATUS_EMOJI 딕셔너리로 변환
    - 병기값(예: "✅/🔵"): 이미 이모지이므로 그대로 반환
    - "—": 그대로 반환
    """
    def _convert(v):
        if pd.isna(v) or str(v) == "—":
            return "—"
        s = str(v)
        # "/" 포함 시 이미 병기 이모지 → 그대로 반환
        if "/" in s:
            return s
        return STATUS_EMOJI.get(s, s)
    return pivot.map(_convert)


def color_log_rows(df: pd.DataFrame):
    """로그 레벨별 배경색 Styler 반환"""
    def _row_color(row):
        color = LOG_LEVEL_COLORS.get(row["레벨"], "#ffffff")
        return [f"background-color: {color}"] * len(row)
    return df.style.apply(_row_color, axis=1)


# ─────────────────────────────────────────────
# 로그 섹션 (fragment — 로그만 부분 새로고침 가능)
# ─────────────────────────────────────────────

@st.fragment
def _render_log_section():
    """최근 로그 섹션. 버튼 클릭 시 전체 페이지 재실행 없이 이 섹션만 재실행."""
    col_title, col_refresh = st.columns([9, 1])
    with col_title:
        st.subheader("📋 최근 로그")
    with col_refresh:
        st.write("")   # 버튼 수직 정렬을 위한 여백
        if st.button("🔄", help="로그만 새로고침"):
            st.rerun(scope="fragment")

    log_files = sorted(LOG_DIR.glob("pipeline_*.log"), reverse=True)[:5]
    log_options = {f.stem.replace("pipeline_", ""): f for f in log_files}

    if log_options:
        col_sel, col_level, col_n = st.columns([2, 2, 2])
        with col_sel:
            selected_date = st.selectbox("날짜 선택", options=list(log_options.keys()))
        with col_level:
            level_filter = st.multiselect(
                "레벨 필터",
                options=["SUCCESS", "ERROR", "WARNING", "INFO", "DEBUG"],
                default=["SUCCESS", "ERROR", "WARNING", "INFO"],
            )
        with col_n:
            n_lines = st.slider("표시 줄 수", min_value=20, max_value=200, value=50, step=10)

        log_entries = parse_log_file(log_options[selected_date], n_lines=n_lines * 3)

        if log_entries:
            log_df = pd.DataFrame(log_entries)
            if level_filter:
                log_df = log_df[log_df["레벨"].isin(level_filter)]
            log_df = log_df.tail(n_lines)

            if not log_df.empty:
                st.dataframe(
                    color_log_rows(log_df),
                    use_container_width=True,
                    hide_index=True,
                    height=400,
                )
            else:
                st.info("선택한 레벨의 로그가 없습니다.")
        else:
            st.info("로그 항목이 없습니다.")
    else:
        st.info("로그 파일이 없습니다.")

    st.caption(f"마지막 로드: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ─────────────────────────────────────────────
# 메인 UI
# ─────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="llm_for_ship 파이프라인 모니터",
        page_icon="🚢",
        layout="wide",
    )

    # 헤더
    st.title("🚢 llm_for_ship 파이프라인 모니터")
    st.caption(
        f"기준 날짜: {date.today()}  |  "
        f"DB: {os.environ.get('DB_NAME', 'ship_env')}@{os.environ.get('DB_HOST', 'localhost')}"
    )

    # DB 연결 확인
    conn_test = _get_conn()
    if conn_test is None:
        st.error("⛔ DB 연결 실패 — Docker Desktop이 실행 중인지 확인하세요.")
        st.stop()
    conn_test.close()

    # 새로고침 버튼
    col_refresh, _ = st.columns([1, 9])
    with col_refresh:
        if st.button("🔄 새로고침"):
            st.rerun()

    st.divider()

    # ── 섹션 1: 오늘 실행 요약 카드 ──────────────────
    st.subheader("📊 오늘 실행 요약")

    run_info = get_last_run_info()
    forecast_info = get_forecast_horizon()
    missing_cnt = get_missing_count()

    min_forecast_days = (
        min(v["actual"] for v in forecast_info.values())
        if forecast_info else 0
    )
    forecast_ok = all(v["ok"] for v in forecast_info.values()) if forecast_info else False

    STATUS_DISPLAY = {
        "success":  "✅ 완료",
        "dry_run":  "🔍 dry-run",
        "running":  "🔄 적재 중",
        "stuck":    "🛑 응답 없음",
        "no_log":   "📭 로그 없음",
        "no_run":   "📭 실행 없음",
    }
    status = run_info["status"]
    status_label = STATUS_DISPLAY.get(status, "알 수 없음")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("실행 시작", run_info["time"], delta=f"모드: {run_info['mode']}")
    with c2:
        st.metric("실행 상태", status_label)
    with c3:
        st.metric(
            "예보 커버리지",
            f"{min_forecast_days}일",
            delta=f"목표: 5~10일",
            delta_color="normal" if forecast_ok else "inverse",
        )
    with c4:
        st.metric(
            "누락 항목 (최근 14일)",
            f"{missing_cnt}건",
            delta_color="inverse" if missing_cnt > 0 else "normal",
        )

    # running / stuck 상태: 마지막 로그 메시지 + 경과 시간 표시
    if status in ("running", "stuck"):
        msg = run_info["last_log_msg"]
        age = run_info["last_log_age"]
        age_str = f"{age}분 전" if age > 0 else "방금"
        last_msg_short = msg[:80] + "…" if len(msg) > 80 else msg

        if status == "stuck":
            st.warning(
                f"🛑 **마지막 응답: {age_str}** ({run_info['last_log_time']})\n\n"
                f"마지막 로그: `{last_msg_short}`"
            )
        else:
            st.info(
                f"🔄 **진행 중 — 마지막 응답: {age_str}** ({run_info['last_log_time']})\n\n"
                f"진행 상황: `{last_msg_short}`"
            )

    st.divider()

    # ── 섹션 2: 예보 커버리지 현황 ──────────────────
    st.subheader("📡 예보 커버리지 현황")

    if forecast_info:
        fc_rows = []
        for table, info in forecast_info.items():
            ok_icon = "✅" if info["ok"] else "⚠️"
            fc_rows.append({
                "데이터":       info["label"],
                "실제 커버":    f"{info['actual']}일",
                "목표 커버":    f"{info['expected']}일",
                "DB 최신 시각": info["max_date"],
                "상태":         f"{ok_icon} {'정상' if info['ok'] else '부족'}",
            })
        fc_df = pd.DataFrame(fc_rows)

        def _highlight_fc(row):
            if "부족" in str(row["상태"]):
                return ["background-color: #fff3cd"] * len(row)
            return [""] * len(row)

        st.dataframe(
            fc_df.style.apply(_highlight_fc, axis=1),
            use_container_width=True,
            hide_index=True,
        )

        if not forecast_ok:
            st.warning(
                "⚠️ 예보 데이터가 목표 기간보다 짧습니다. "
                "아래 명령으로 전체 기간을 재수집하세요:\n\n"
                "```bash\nuv run python run.py --mode forecast_only\n```"
            )
    else:
        st.info("예보 테이블 정보를 불러올 수 없습니다.")

    st.divider()

    # ── 섹션 3: 데이터 커버리지 달력 ────────────────
    st.subheader("📅 데이터 커버리지 달력")

    # 기본 30일, "더 보기" 버튼으로 30일씩 추가
    if "cov_days" not in st.session_state:
        st.session_state.cov_days = 30

    cov_df = get_coverage(days=st.session_state.cov_days)

    today = date.today()
    future_days  = 10  # ECMWF 최대 예보 기간
    past_dates   = [today - timedelta(days=i) for i in range(st.session_state.cov_days)]
    future_dates = [today + timedelta(days=i) for i in range(1, future_days + 1)]

    # ── 과거 날짜: pipeline_coverage → {(date, col): dict} ──
    # dict 키: status, time_kst, download_status, load_status
    reanalysis_status: dict[tuple, dict] = {}
    if not cov_df.empty:
        for _, row in cov_df.iterrows():
            raw_status = STATUS_NORMALIZE.get(row["status"], row["status"])

            # loaded_at → KST 시각 문자열
            loaded_at = row.get("loaded_at")
            if loaded_at is not None and not pd.isnull(loaded_at):
                try:
                    if hasattr(loaded_at, "tzinfo") and loaded_at.tzinfo:
                        kst = loaded_at.astimezone(timezone(timedelta(hours=9)))
                    else:
                        kst = loaded_at + timedelta(hours=9)
                    time_kst = kst.strftime("%H:%M")
                except Exception:
                    time_kst = None
            else:
                time_kst = None

            # download_status / load_status (Phase 17: None이면 레거시 레코드)
            dl_st = row.get("download_status") if "download_status" in row.index else None
            ld_st = row.get("load_status")     if "load_status"     in row.index else None

            for col_name in SOURCE_TO_COL.get(row["source"], []):
                reanalysis_status[(row["date"], col_name)] = {
                    "status":          raw_status,
                    "time_kst":        time_kst,
                    "download_status": dl_st,
                    "load_status":     ld_st,
                }

    # ── 미래 날짜: 예보 테이블 날짜 집합 ──
    forecast_date_sets = get_forecast_dates()  # {"Wind": {date,...}, ...}

    # ── 전체 행 구성 ──
    all_rows = []

    # 미래 날짜: 예보 데이터만 존재
    for d in future_dates:
        for col in COVERAGE_COLUMN_ORDER:
            has_fc = d in forecast_date_sets.get(col, set())
            all_rows.append({
                "date":   d,
                "source": col,
                "status": STATUS_EMOJI.get("forecast", "🔵") if has_fc else "—",
            })

    # 과거/오늘 날짜: 재분석 + 예보 동시 존재 시 병기
    for d in past_dates:
        for col in COVERAGE_COLUMN_ORDER:
            ra_entry = reanalysis_status.get((d, col))  # dict 또는 None
            has_fc   = d in forecast_date_sets.get(col, set())

            if ra_entry:
                dl_st    = ra_entry["download_status"]
                ld_st    = ra_entry["load_status"]
                ra_st    = ra_entry["status"]
                time_kst = ra_entry["time_kst"]

                # Phase 17: download_status/load_status가 있으면 정밀 이모지 사용
                if dl_st is not None or ld_st is not None:
                    dl_key   = dl_st if dl_st else ("complete" if ra_st == "complete" else "failed")
                    base_emoji = DL_LOAD_EMOJI.get((dl_key, ld_st), STATUS_EMOJI.get(ra_st, ra_st))
                else:
                    # 레거시 레코드: 기존 status 이모지
                    base_emoji = STATUS_EMOJI.get(ra_st, ra_st)

                # 완료 상태에는 KST 시각 병기
                ra_cell = f"{base_emoji} {time_kst}" if (time_kst and "✅" in base_emoji) else base_emoji

                if has_fc:
                    # 재분석 + 예보 동시 존재 → 이모지 병기
                    cell_val = f"{ra_cell}/{STATUS_EMOJI.get('forecast', '🔵')}"
                else:
                    cell_val = ra_cell
            elif has_fc:
                cell_val = STATUS_EMOJI.get("forecast", "🔵")
            else:
                cell_val = STATUS_EMOJI.get("missing", "❌")

            all_rows.append({"date": d, "source": col, "status": cell_val})

    full_df = pd.DataFrame(all_rows)

    # 피벗: 날짜(행) × 소스(열), 최신 날짜 상단
    pivot = full_df.pivot_table(
        index="date", columns="source", values="status", aggfunc="first"
    ).sort_index(ascending=False)
    pivot = pivot[COVERAGE_COLUMN_ORDER]

    st.caption(f"미래 {future_days}일(예보) + 과거 {st.session_state.cov_days}일(재분석) 표시 중  |  완료 시각은 KST 기준")
    st.dataframe(pivot, use_container_width=True)

    # 더 보기 / 초기화 버튼
    col_more, col_reset, _ = st.columns([1, 1, 8])
    with col_more:
        if st.button("📂 30일 더 보기"):
            st.session_state.cov_days += 30
            st.rerun()
    with col_reset:
        if st.session_state.cov_days > 30 and st.button("🔼 초기화 (30일)"):
            st.session_state.cov_days = 30
            st.rerun()

    # 범례 (Phase 17: 다운로드/적재 분리 상태 이모지 추가)
    ALL_LEGEND = {
        **STATUS_EMOJI,
        "다운O/적재X": "📥🔴",
        "다운O/적재부분": "📥⚠️",
        "건너뜀": "⏭️",
    }
    legend_cols = st.columns(len(ALL_LEGEND))
    for i, (lbl, emoji) in enumerate(ALL_LEGEND.items()):
        with legend_cols[i]:
            st.caption(f"{emoji} {lbl}")

    st.divider()

    # ── 섹션 3-B: STEP 5 재시도 필요 항목 ────────────────
    st.subheader("🔁 재시도 필요 항목 (STEP 5)")

    retry_df = _fetch_df("""
        SELECT date, source, status, download_status, load_status, loaded_at, notes
        FROM   pipeline_coverage
        WHERE  date >= %s
          AND  status NOT IN ('complete', 'permanent_forecast')
          AND  COALESCE(download_status, '') != 'skipped'
          AND  COALESCE(load_status, '')     != 'skipped'
        ORDER BY date DESC, source
    """, (date.today() - timedelta(days=14),))

    if retry_df is not None and not retry_df.empty:
        # retry_type 열 추가 (download / load_only)
        def _retry_type(row):
            if row.get("download_status") == "complete":
                return "load_only"
            return "download"
        retry_df["재시도 유형"] = retry_df.apply(_retry_type, axis=1)

        # 표시 컬럼 정리
        display_cols = ["date", "source", "status", "download_status", "load_status", "재시도 유형"]
        display_df   = retry_df[display_cols].rename(columns={
            "date":            "날짜",
            "source":          "소스",
            "status":          "전체 상태",
            "download_status": "다운로드",
            "load_status":     "적재",
        })

        def _highlight_retry(row):
            if row["재시도 유형"] == "download":
                return ["background-color: #f8d7da"] * len(row)   # 빨간 배경 (다운 필요)
            return ["background-color: #fff3cd"] * len(row)       # 노란 배경 (적재만 필요)

        st.caption(
            f"최근 14일 내 미완료 항목 {len(display_df)}건  |  "
            "🔴 = 다운로드 재시도 필요  🟡 = 적재만 재시도 필요"
        )
        st.dataframe(
            display_df.style.apply(_highlight_retry, axis=1),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("✅ 재시도 필요 항목 없음 (최근 14일)")

    st.divider()

    # ── 섹션 4: DB 테이블 현황 ───────────────────────
    st.subheader("🗄️ DB 테이블 현황")

    stats_df = get_table_stats()

    if not stats_df.empty:
        reanalysis_df = stats_df[stats_df["구분"] == "재분석"].drop(columns="구분")
        forecast_df   = stats_df[stats_df["구분"] == "예보"].drop(columns="구분")

        col_l, col_r = st.columns(2)
        with col_l:
            st.markdown("**재분석 (과거 데이터)**")
            st.dataframe(reanalysis_df, use_container_width=True, hide_index=True)
        with col_r:
            st.markdown("**예보 (미래 데이터)**")
            st.dataframe(forecast_df, use_container_width=True, hide_index=True)
    else:
        st.info("테이블 통계를 불러올 수 없습니다.")

    st.divider()

    # ── 섹션 5: 최근 로그 ───────────────────────────
    _render_log_section()


main()
