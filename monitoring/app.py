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

# pipeline_coverage source → 표시 컬럼명 매핑
# ecmwf_reanalysis는 Wind+Wave 통합 소스이므로 두 컬럼으로 분리 표시
SOURCE_SPLIT = {
    # source          → 표시할 컬럼명 리스트 (1개 또는 2개)
    "ecmwf_reanalysis": ["Wind 재분석",    "Wave 재분석"],
    "hycom_current":    ["Current 재분석"],
    "ecmwf_forecast":   ["Wind 예보"],
    "noaa_forecast":    ["Wave 예보"],
    "hycom_forecast":   ["Current 예보"],
}

# 컬럼 표시 순서 (재분석 3개 → 예보 3개)
COVERAGE_COLUMN_ORDER = [
    "Wind 재분석", "Wave 재분석", "Current 재분석",
    "Wind 예보",   "Wave 예보",   "Current 예보",
]

# 상태별 이모지
STATUS_EMOJI = {
    "complete":           "✅",
    "partial":            "⚠️",
    "forecast_only":      "🔵",
    "permanent_forecast": "🟣",
    "missing":            "❌",
    "failed":             "🔴",
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


def get_coverage(days: int = 14) -> pd.DataFrame:
    """pipeline_coverage 최근 N일 조회"""
    start = date.today() - timedelta(days=days - 1)
    return _fetch_df(
        """
        SELECT date, source, status, row_count
        FROM   pipeline_coverage
        WHERE  date >= %s
        ORDER  BY date DESC, source
        """,
        (start,),
    )


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


def get_last_run_info() -> dict:
    """오늘 로그 파일에서 마지막 파이프라인 실행 정보 추출"""
    today_str = date.today().strftime("%Y-%m-%d")
    log_path = LOG_DIR / f"pipeline_{today_str}.log"

    if not log_path.exists():
        return {"status": "no_log", "time": "-", "mode": "-", "dry_run": False}

    content = log_path.read_text(encoding="utf-8", errors="ignore")

    starts = list(re.finditer(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
        r"파이프라인 시작 \| 모드: (\S+) \| dry_run: (\S+)",
        content,
    ))
    completes = list(re.finditer(r"파이프라인 완료", content))
    has_error = bool(re.search(r"\| ERROR\s*\|", content))

    if not starts:
        return {"status": "no_run", "time": "-", "mode": "-", "dry_run": False}

    last = starts[-1]
    run_time = last.group(1)
    mode = last.group(2)
    dry_run = last.group(3) == "True"

    if dry_run:
        status = "dry_run"
    elif completes:
        status = "success"
    elif has_error:
        status = "error"
    else:
        status = "running"

    return {"status": status, "time": run_time, "mode": mode, "dry_run": dry_run}


# ─────────────────────────────────────────────
# Styler 헬퍼
# ─────────────────────────────────────────────

def style_coverage_pivot(pivot: pd.DataFrame) -> pd.DataFrame:
    """커버리지 피벗 테이블: 상태값 → 이모지로 변환"""
    return pivot.map(
        lambda v: STATUS_EMOJI.get(str(v), str(v)) if pd.notna(v) else "—"
    )


def color_log_rows(df: pd.DataFrame):
    """로그 레벨별 배경색 Styler 반환"""
    def _row_color(row):
        color = LOG_LEVEL_COLORS.get(row["레벨"], "#ffffff")
        return [f"background-color: {color}"] * len(row)
    return df.style.apply(_row_color, axis=1)


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
        "success":  "✅ 성공",
        "dry_run":  "🔍 dry-run",
        "running":  "🔄 실행 중",
        "error":    "❌ 오류",
        "no_log":   "📭 로그 없음",
        "no_run":   "📭 실행 없음",
    }
    status_label = STATUS_DISPLAY.get(run_info["status"], "알 수 없음")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("마지막 실행", run_info["time"], delta=f"모드: {run_info['mode']}")
    with c2:
        st.metric("실행 결과", status_label)
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
    st.subheader("📅 데이터 커버리지 달력 (최근 14일)")

    cov_df = get_coverage(days=14)

    # 최근 14일 날짜 목록 생성 (데이터 유무와 무관하게 모든 날짜 표시)
    today = date.today()
    all_dates = [today - timedelta(days=i) for i in range(14)]

    # ecmwf_reanalysis → Wind 재분석 / Wave 재분석 두 컬럼으로 분리
    expanded_rows = []
    if not cov_df.empty:
        for _, row in cov_df.iterrows():
            col_names = SOURCE_SPLIT.get(row["source"], [row["source"]])
            for col_name in col_names:
                expanded_rows.append({
                    "date":   row["date"],
                    "source": col_name,
                    "status": row["status"],
                })

    # 6개 컬럼 × 14일을 "missing"으로 초기화한 뒤 실제 데이터로 덮어쓰기
    base_rows = [
        {"date": d, "source": col, "status": "missing"}
        for d in all_dates
        for col in COVERAGE_COLUMN_ORDER
    ]
    base_df = pd.DataFrame(base_rows)

    if expanded_rows:
        actual_df = pd.DataFrame(expanded_rows)
        # 실제 데이터 병합 (실제 값이 있으면 "missing" 덮어쓰기)
        base_df = base_df.merge(
            actual_df.rename(columns={"status": "actual_status"}),
            on=["date", "source"],
            how="left",
        )
        base_df["status"] = base_df["actual_status"].combine_first(base_df["status"])
        base_df = base_df[["date", "source", "status"]]

    # 피벗: 날짜(행) × 소스(열), 최신 날짜 상단
    pivot = base_df.pivot_table(
        index="date", columns="source", values="status", aggfunc="first"
    ).sort_index(ascending=False)

    # 컬럼 순서 고정 (항상 6개 컬럼)
    pivot = pivot[COVERAGE_COLUMN_ORDER]

    st.dataframe(style_coverage_pivot(pivot), use_container_width=True)

    # 범례
    legend_cols = st.columns(len(STATUS_EMOJI))
    for i, (status, emoji) in enumerate(STATUS_EMOJI.items()):
        with legend_cols[i]:
            st.caption(f"{emoji} {status}")

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
    st.subheader("📋 최근 로그")

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


main()
