"""
NetCDF → TimescaleDB 데이터 적재 모듈
======================================

NetCDF 파일을 읽어서 TimescaleDB에 빠르게 적재함.

적재 방식: PostgreSQL COPY + 임시 테이블 → INSERT ON CONFLICT
  - COPY로 임시(TEMP) 테이블에 전체 데이터를 빠르게 올린 뒤
  - INSERT ... ON CONFLICT 로 본 테이블에 이관
  - 재분석 파일(wind, wave): ON CONFLICT DO UPDATE SET (자기 컬럼만 갱신)
    → wind 파일이 먼저 들어온 후 wave 파일이 wave 컬럼만 업데이트하는 방식
  - 예보 파일: ON CONFLICT DO NOTHING (동일 발행일+시각+위치 중복 무시)

이 방식의 장점:
  - wind/wave 두 파일이 같은 테이블(env_ecmwf_reanalysis)에 분리 적재 가능
  - ERA5T → ERA5 교체(UPSERT) 지원
  - 재시도 시 중복 없이 안전하게 적재
"""

import io              # 메모리 내 버퍼 (파일 없이 CSV 데이터를 메모리에서 처리)
from pathlib import Path
import xarray as xr    # NetCDF 파일 읽기 라이브러리 (기상/해양 데이터 표준)
import pandas as pd    # 데이터를 표 형태(DataFrame)로 처리
import numpy as np     # 수치 연산 (NaN 처리 등)
from loguru import logger

from .connection import get_connection


# ─────────────────────────────────────────────────────
# 테이블별 컬럼 정의
# 파일명으로 어느 테이블에 적재할지 자동 판단
# ─────────────────────────────────────────────────────

# ECMWF wind 파일 컬럼 (ecmwf_wind_YYYYMMDD.nc)
ECMWF_WIND_COLUMNS = [
    "datetime", "lat", "lon",
    "u10",   # 동서 풍속 (m/s)
    "v10",   # 남북 풍속 (m/s)
]

# ECMWF wave 파일 컬럼 (ecmwf_wave_YYYYMMDD.nc)
ECMWF_WAVE_COLUMNS = [
    "datetime", "lat", "lon",
    "swh",   # 복합 유의파고
    "mwd",   # 평균 파랑 방향
    "mwp",   # 평균 파랑 주기
    "shts",  # 너울 유의파고
    "mdts",  # 너울 방향
    "mpts",  # 너울 주기
    "shww",  # 풍파 유의파고
    "mdww",  # 풍파 방향
    "mpww",  # 풍파 주기
]

# HYCOM current 파일 컬럼 (hycom_current_YYYYMMDD.nc)
HYCOM_CURRENT_COLUMNS = [
    "datetime", "lat", "lon",
    "water_u",  # 동서 해류 속도 (m/s)
    "water_v",  # 남북 해류 속도 (m/s)
]

# ─────────────────────────────────────────────────────
# 예보 테이블 컬럼 정의 (issued_at 컬럼 포함)
# ─────────────────────────────────────────────────────

# ECMWF 예보 wind 파일 컬럼 (ecmwf_fc_wind_YYYYMMDD.nc)
# ※ 파랑 변수는 NOAA WW3에서 전담 → NOAA_FC_WAVE_COLUMNS 참조
ECMWF_FC_WIND_COLUMNS = [
    "issued_at", "datetime", "lat", "lon",  # issued_at: 예보 발행 시각
    "u10",   # 동서 풍속 (m/s)
    "v10",   # 남북 풍속 (m/s)
]

# HYCOM 예보 current 파일 컬럼 (hycom_fc_current_YYYYMMDD.nc)
HYCOM_FC_COLUMNS = [
    "issued_at", "datetime", "lat", "lon",
    "water_u",   # 동서 해류 속도 (m/s)
    "water_v",   # 남북 해류 속도 (m/s)
]

# NOAA WW3 파랑 예보 파일 컬럼 (noaa_fc_wave_YYYYMMDD.nc)
# 변수명은 다운로더에서 ECMWF 컬럼명으로 미리 변환됨 (Thgt→swh 등)
NOAA_FC_WAVE_COLUMNS = [
    "issued_at", "datetime", "lat", "lon",
    "swh",   # 복합 유의파고 (m)   ← NOAA Thgt
    "mwd",   # 복합 파랑 방향 (°) ← NOAA Tdir
    "mwp",   # 복합 파랑 주기 (s) ← NOAA Tper
    "shts",  # 너울 유의파고 (m)  ← NOAA shgt
    "mdts",  # 너울 방향 (°)     ← NOAA sdir
    "mpts",  # 너울 주기 (s)     ← NOAA sper
    "shww",  # 풍파 유의파고 (m)  ← NOAA whgt
    "mdww",  # 풍파 방향 (°)     ← NOAA wdir
    "mpww",  # 풍파 주기 (s)     ← NOAA wper
]

# ─────────────────────────────────────────────────────
# 테이블 PK 및 UPSERT 정책 정의
# ─────────────────────────────────────────────────────

# 테이블별 PRIMARY KEY 컬럼 목록
# Phase 14-B: 예보 테이블 PK를 (datetime, lat, lon) 으로 변경
#   - 구: (issued_at, datetime, lat, lon) → 매일 새 행 추가, 누적
#   - 신: (datetime, lat, lon)            → 동일 유효시각 = 최신 예보로 UPSERT
TABLE_PK = {
    "env_ecmwf_reanalysis": ["datetime", "lat", "lon"],
    "env_hycom_current":    ["datetime", "lat", "lon"],
    "env_ecmwf_forecast":   ["datetime", "lat", "lon"],  # Phase 14-B 변경
    "env_hycom_forecast":   ["datetime", "lat", "lon"],  # Phase 14-B 변경
    "env_noaa_forecast":    ["datetime", "lat", "lon"],  # Phase 14-B 변경
}

# 파일 유형별 ON CONFLICT 시 갱신할 컬럼 목록
# list → ON CONFLICT DO UPDATE SET (해당 컬럼 갱신)
# Phase 14-B: 예보 파일은 issued_at + 변수 컬럼 갱신 (최신 예보로 덮어쓰기)
UPSERT_UPDATE_COLS = {
    "ecmwf_wind":     ["u10", "v10"],                             # 재분석 wind: wind 컬럼만 갱신
    "ecmwf_wave":     ["swh", "mwd", "mwp",                      # 재분석 wave: wave 컬럼만 갱신
                       "shts", "mdts", "mpts",
                       "shww", "mdww", "mpww"],
    "hycom_current":  ["water_u", "water_v"],                     # HYCOM 분석: 해류 컬럼만 갱신
    "ecmwf_fc_wind":  ["issued_at", "u10", "v10"],                # 예보: issued_at + 변수 갱신
    "hycom_fc_current": ["issued_at", "water_u", "water_v"],      # 예보: issued_at + 변수 갱신
    "noaa_fc_wave":   ["issued_at",                               # 예보: issued_at + 변수 갱신
                       "swh", "mwd", "mwp",
                       "shts", "mdts", "mpts",
                       "shww", "mdww", "mpww"],
}

# 임시 테이블 컬럼 타입 정의 (TEMP TABLE 생성용)
COLUMN_TYPES = {
    "datetime":  "TIMESTAMPTZ",
    "issued_at": "TIMESTAMPTZ",
    "lat":       "REAL",
    "lon":       "REAL",
    "u10":       "REAL",
    "v10":       "REAL",
    "swh":       "REAL",
    "mwd":       "REAL",
    "mwp":       "REAL",
    "shts":      "REAL",
    "mdts":      "REAL",
    "mpts":      "REAL",
    "shww":      "REAL",
    "mdww":      "REAL",
    "mpww":      "REAL",
    "water_u":   "REAL",
    "water_v":   "REAL",
}


def _find_latest_reanalysis_wave(fc_wind_path: Path) -> Path | None:
    """
    예보 wind 파일의 해양 필터링에 사용할 재분석 wave 파일 중 가장 최근 것을 반환.

    육지/해양 경계는 시간에 관계없이 고정된 정보이므로,
    예보 wind 적재 시 가장 최근 재분석 wave 파일의 마스크를 재사용해도 무방함.

    경로 구조 가정:
      fc_wind_path: data/ecmwf/forecast/YYYY/MM/ecmwf_fc_wind_YYYYMMDD.nc
      재분석 경로:   data/ecmwf/reanalysis/**/*.nc
        (forecast 폴더 기준 3단계 상위의 형제 폴더 reanalysis/)

    Parameters
    ----------
    fc_wind_path : Path  →  예보 wind nc 파일 경로

    Returns
    -------
    Path | None  →  가장 최근 재분석 wave 파일 경로, 없으면 None
    """
    # data/ecmwf/ 폴더 = forecast/YYYY/MM/ 기준 3단계 상위
    ecmwf_base     = fc_wind_path.parent.parent.parent.parent
    reanalysis_dir = ecmwf_base / "reanalysis"

    if not reanalysis_dir.exists():
        logger.debug(f"  재분석 폴더 없음: {reanalysis_dir}")
        return None

    # 재분석 wave 파일 전체 탐색 (파일명 기준 정렬 → 마지막이 가장 최근)
    wave_files = sorted(reanalysis_dir.glob("**/ecmwf_wave_*.nc"))

    if not wave_files:
        logger.debug(f"  재분석 wave 파일 없음: {reanalysis_dir}")
        return None

    return wave_files[-1]   # 파일명 날짜순 정렬 → 마지막 = 가장 최근


def _detect_table_config(nc_path: Path) -> tuple[str, list[str], str, list[str] | None]:
    """
    파일명을 보고 어느 테이블에 적재할지, 컬럼, 파일 유형, UPSERT 갱신 컬럼을 자동 판단

    파일명 규칙:
      ecmwf_wind_YYYYMMDD.nc      → env_ecmwf_reanalysis (wind 컬럼)
      ecmwf_wave_YYYYMMDD.nc      → env_ecmwf_reanalysis (wave 컬럼)
      hycom_current_YYYYMMDD.nc   → env_hycom_current
      ecmwf_fc_wind_YYYYMMDD.nc   → env_ecmwf_forecast
      hycom_fc_current_YYYYMMDD.nc → env_hycom_forecast
      noaa_fc_wave_YYYYMMDD.nc    → env_noaa_forecast

    Returns
    -------
    tuple: (테이블명, 컬럼목록, 파일유형키, UPSERT갱신컬럼)
    """
    filename = nc_path.name.lower()

    # ── 예보 파일 먼저 검사 (재분석 파일명 패턴과 혼동 방지) ──
    # Phase 14-B: 예보 파일은 UPSERT_UPDATE_COLS 에서 갱신 컬럼 목록 가져옴
    if "ecmwf_fc_wind" in filename:
        return "env_ecmwf_forecast", ECMWF_FC_WIND_COLUMNS, "ecmwf_fc_wind", UPSERT_UPDATE_COLS["ecmwf_fc_wind"]
    elif "hycom_fc_current" in filename:
        return "env_hycom_forecast", HYCOM_FC_COLUMNS, "hycom_fc_current", UPSERT_UPDATE_COLS["hycom_fc_current"]
    elif "noaa_fc_wave" in filename:
        return "env_noaa_forecast", NOAA_FC_WAVE_COLUMNS, "noaa_fc_wave", UPSERT_UPDATE_COLS["noaa_fc_wave"]

    # ── 재분석/분석 파일 ──
    elif "ecmwf_wind" in filename:
        return "env_ecmwf_reanalysis", ECMWF_WIND_COLUMNS, "ecmwf_wind", UPSERT_UPDATE_COLS["ecmwf_wind"]
    elif "ecmwf_wave" in filename:
        return "env_ecmwf_reanalysis", ECMWF_WAVE_COLUMNS, "ecmwf_wave", UPSERT_UPDATE_COLS["ecmwf_wave"]
    elif "hycom_current" in filename:
        return "env_hycom_current", HYCOM_CURRENT_COLUMNS, "hycom_current", UPSERT_UPDATE_COLS["hycom_current"]
    else:
        raise ValueError(
            f"파일명으로 테이블을 판단할 수 없습니다: {nc_path.name}\n"
            f"재분석: ecmwf_wind_*, ecmwf_wave_*, hycom_current_*\n"
            f"예보:   ecmwf_fc_wind_*, hycom_fc_current_*, noaa_fc_wave_*"
        )


def _load_by_timesteps(
    ds: "xr.Dataset",
    nc_path: Path,
    file_type: str,
    table_columns: list,
    target_table: str,
    issued_at,
) -> int:
    """
    대용량 예보 파일(HYCOM 해류, NOAA 파랑)을 시간 스텝별로 분할 처리

    전체 데이터셋을 한꺼번에 DataFrame으로 변환하면 수 GB 메모리가 필요하여 OOM 발생.
    시간 스텝 1개씩 처리하면 메모리 사용량이 1/스텝수 수준으로 줄어듦.

    처리 흐름:
      1) TRUNCATE 테이블 (기존 예보 전체 삭제)
      2) 시간 스텝별 반복:
           ds.isel(time=i) → DataFrame → 컬럼 정리 → COPY to DB
      3) 모든 스텝 완료 후 commit

    Parameters
    ----------
    ds           : xr.Dataset  →  이미 열린 NetCDF 데이터셋 (lazy load 상태)
    nc_path      : Path        →  파일 경로 (로그용)
    file_type    : str         →  "hycom_fc_current" 또는 "noaa_fc_wave"
    table_columns: list        →  DB 테이블 컬럼 목록 (순서 중요)
    target_table : str         →  적재 대상 테이블명
    issued_at    : pd.Timestamp|None  →  예보 발행 시각

    Returns
    -------
    int  →  총 적재 행 수
    """
    # 시간 차원 추출 (HYCOM/NOAA 모두 "time" 사용)
    time_dim = "time"
    n_steps  = len(ds[time_dim])

    logger.info(f"  시간 스텝별 분할 처리 시작: {n_steps}개 스텝 (OOM 방지)")

    conn = get_connection()
    total_loaded = 0

    try:
        cursor  = conn.cursor()
        col_str = ", ".join(table_columns)

        # ── TRUNCATE: 기존 예보 전체 삭제 (최신 예보로 완전 교체) ──
        logger.info(f"  예보 테이블 초기화 (TRUNCATE): {target_table}")
        cursor.execute(f"TRUNCATE TABLE {target_table}")

        for step_idx in range(n_steps):
            # ── 시간 스텝 1개 선택 → DataFrame 변환 ──
            # isel(time=i): 해당 시각의 2D 공간 데이터만 선택 (전체의 1/n_steps)
            ds_step  = ds.isel({time_dim: step_idx})
            step_df  = ds_step.to_dataframe().reset_index()

            # ── 컬럼명 정리: HYCOM/NOAA 좌표명 → 공통 컬럼명 ──
            rename_map = {}
            if "valid_time" in step_df.columns:
                rename_map["valid_time"] = "datetime"
            elif "time" in step_df.columns:
                rename_map["time"] = "datetime"
            if "latitude" in step_df.columns:
                rename_map["latitude"] = "lat"
            if "longitude" in step_df.columns:
                rename_map["longitude"] = "lon"
            step_df = step_df.rename(columns=rename_map)

            # ── issued_at 컬럼 추가 ──
            if "issued_at" in table_columns:
                if issued_at is not None:
                    step_df["issued_at"] = issued_at
                else:
                    step_df["issued_at"] = pd.Timestamp.now(tz="UTC")

            # ── NOAA wave: NaN 행 제거 (육지 격자 = 모든 파랑 변수가 NaN) ──
            # NOAA WW3는 육지에 NaN이 들어 있으므로 swh NaN 행을 제거
            if file_type == "noaa_fc_wave" and "swh" in step_df.columns:
                step_df = step_df.dropna(subset=["swh"])

            # ── 필요한 컬럼만 선택 (없는 컬럼은 NaN으로 채움) ──
            for col in table_columns:
                if col not in step_df.columns:
                    step_df[col] = np.nan
            step_df = step_df[table_columns]

            # ── 시간 컬럼 타임존 처리 (UTC) ──
            step_df["datetime"] = pd.to_datetime(step_df["datetime"], utc=True)
            if "issued_at" in step_df.columns:
                step_df["issued_at"] = pd.to_datetime(step_df["issued_at"], utc=True)

            # ── COPY to DB ──
            step_rows = len(step_df)
            buffer    = io.StringIO()
            step_df.to_csv(buffer, index=False, header=False, na_rep="\\N")
            buffer.seek(0)
            cursor.copy_expert(
                f"COPY {target_table} ({col_str}) FROM STDIN "
                f"WITH (FORMAT csv, NULL '\\N')",
                buffer,
            )
            total_loaded += step_rows
            logger.debug(
                f"  스텝 {step_idx + 1}/{n_steps} 완료 | "
                f"{step_rows:,}행 | 누적 {total_loaded:,}행"
            )

        # 모든 스텝 완료 후 1회 커밋
        conn.commit()
        cursor.close()

        logger.success(
            f"[적재 완료] {nc_path.name} | 총 {total_loaded:,}행 적재"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"[적재 실패] {nc_path.name}: {e}")
        raise

    finally:
        ds.close()     # NetCDF 파일 닫기
        conn.close()   # DB 연결 닫기

    return total_loaded


def load_netcdf_to_db(
    nc_path: Path,
    table_name: str = None,   # None이면 파일명으로 자동 판단
    batch_size: int = 50_000, # 사용되지 않음 (하위 호환 유지용)
) -> int:
    """
    NetCDF 파일 1개를 읽어서 TimescaleDB에 전체 적재

    적재 방식: COPY to TEMP → INSERT ON CONFLICT (단일 트랜잭션)
      - 임시 테이블에 전체 데이터를 한 번에 COPY (배치 루프 없음)
      - INSERT ... ON CONFLICT 로 본 테이블에 한 번에 이관
      - 커밋 1회 → 오버헤드 최소화

      - 재분석 파일: ON CONFLICT DO UPDATE (자기 컬럼만 갱신)
        → wind 파일 먼저 적재 후 wave 파일이 wave 컬럼을 업데이트하는 방식
      - 예보 파일: ON CONFLICT DO NOTHING (동일 발행일 중복 무시)

    Parameters
    ----------
    nc_path    : Path   →  적재할 NetCDF 파일 경로
    table_name : str    →  테이블 이름 (None이면 파일명으로 자동 판단)
    batch_size : int    →  미사용 (API 호환성 유지용)

    Returns
    -------
    int  →  실제로 적재/갱신된 총 행 수
    """
    # 파일명으로 테이블, 컬럼, 파일유형, UPSERT 정책 자동 판단
    auto_table, table_columns, file_type, update_cols = _detect_table_config(nc_path)

    # 명시적으로 테이블명이 전달된 경우 그것을 우선 사용
    target_table = table_name if table_name else auto_table

    # PK 컬럼 목록 조회
    pk_cols = TABLE_PK[target_table]

    logger.info(f"[적재 시작] {nc_path.name} → {target_table}")

    # ── 1단계: NetCDF 파일 읽기 ──
    logger.debug("  NetCDF 파일 로딩 중...")
    # NOAA WW3 파일: 파랑 주기 변수(mpts, mpww)가 units="wave_seconds"로 저장되나
    # xarray가 timedelta64로 오해석할 수 있으므로 decode_timedelta=False 적용
    is_noaa = "noaa_fc" in nc_path.name.lower()
    ds = xr.open_dataset(
        nc_path,
        decode_times=True,
        decode_timedelta=False if is_noaa else None,
    )

    # ── 1.5단계: 예보 파일인 경우 issued_at 전역 속성 추출 ──
    issued_at = None
    if "issued_at" in ds.attrs:
        issued_at = pd.to_datetime(ds.attrs["issued_at"], utc=True)
        logger.debug(f"  issued_at 읽기 완료: {issued_at}")

    # ── 2단계: 데이터셋을 DataFrame으로 변환 ──
    # HYCOM 해류 예보 / NOAA 파랑 예보처럼 시간 스텝이 많은 대용량 파일은
    # 전체를 한꺼번에 DataFrame으로 변환하면 OOM 발생.
    # → 시간 스텝 1개씩 처리하는 전용 경로(_load_by_timesteps)로 분기.
    TIME_CHUNK_FILE_TYPES = {"hycom_fc_current", "noaa_fc_wave"}
    if file_type in TIME_CHUNK_FILE_TYPES:
        # issued_at은 이미 위에서 추출됨 (1.5단계)
        # ds.close()는 _load_by_timesteps 내부 finally 블록에서 처리됨
        return _load_by_timesteps(
            ds, nc_path, file_type, table_columns, target_table, issued_at
        )

    logger.debug("  DataFrame 변환 중...")
    df = ds.to_dataframe().reset_index()

    # ── 3단계: 컬럼명 정리 ──
    # 파일마다 좌표 컬럼명이 다를 수 있어 통일
    rename_map = {}
    if "valid_time" in df.columns:
        rename_map["valid_time"] = "datetime"   # 예보/새 ERA5 API
    elif "time" in df.columns:
        rename_map["time"] = "datetime"          # HYCOM 및 구 ERA5
    if "latitude" in df.columns:
        rename_map["latitude"] = "lat"
    if "longitude" in df.columns:
        rename_map["longitude"] = "lon"
    df = df.rename(columns=rename_map)

    # ── 3.5단계: 예보 파일인 경우 issued_at 컬럼 추가 ──
    if "issued_at" in table_columns:
        if issued_at is not None:
            df["issued_at"] = issued_at
        else:
            logger.warning(
                f"  issued_at을 파일에서 읽지 못함 → 현재 UTC 시각 사용: {nc_path.name}"
            )
            df["issued_at"] = pd.Timestamp.now(tz="UTC")

    # ── 3.6단계 (ecmwf_wind / ecmwf_fc_wind): 해양 격자만 남기기 ──────────
    #
    # ERA5 wind(u10, v10)는 전 지구 격자(육지+바다)에 모두 값이 존재.
    # 선박은 바다에서만 운항하므로 육지 격자는 불필요 → DB 용량 ~44% 절감.
    #
    # 해양 판단 기준:
    #   swh(유의파고) 변수가 NaN이 아닌 격자 = 해양 격자.
    #   (wave 데이터는 육지 격자에 NaN, 해양 격자에 값이 있는 특성 활용)
    #   육지/해양 경계는 시간에 관계없이 고정된 정보이므로 날짜가 달라도 무방.
    #
    # 마스크 파일 선택:
    #   - ecmwf_wind (재분석):   같은 날짜의 ecmwf_wave_YYYYMMDD.nc
    #   - ecmwf_fc_wind (예보):  재분석 폴더에서 가장 최근 ecmwf_wave_*.nc
    #     (예보 wind는 오늘 날짜 → 재분석은 5~7일 지연 → 같은 날짜 파일 없음)
    #
    # 처리 흐름:
    #   1. 마스크 wave 파일 경로 결정
    #   2. wave 파일에서 swh의 첫 번째 시간 스텝 추출 (경계는 시간과 무관)
    #   3. 해양 (lat, lon) 쌍 목록 생성
    #   4. wind DataFrame에서 해양 좌표만 inner join으로 필터링
    # ─────────────────────────────────────────────────────────────────────────
    if file_type in ("ecmwf_wind", "ecmwf_fc_wind"):
        if file_type == "ecmwf_wind":
            # 재분석 wind: 같은 폴더, 파일명만 wind → wave 교체
            wave_path = nc_path.with_name(nc_path.name.replace("wind", "wave"))
        else:
            # 예보 wind: 재분석 폴더에서 가장 최근 wave 파일 탐색
            wave_path = _find_latest_reanalysis_wave(nc_path)
            if wave_path is not None:
                logger.debug(f"  예보 wind 마스크 소스: {wave_path.name} (재분석 최신)")

        if wave_path is not None and wave_path.exists():
            logger.debug("  해양 격자 마스크 추출 중 (wave 파일: swh 기준)...")
            ds_wave = xr.open_dataset(wave_path, decode_times=True)

            # swh 변수의 첫 번째 시간 스텝만 사용
            # 육지/해양 경계는 모든 시간 스텝에서 동일하므로 첫 스텝으로 충분
            swh_var = ds_wave["swh"]
            time_dim = swh_var.dims[0]          # 첫 번째 차원 = 시간
            swh_first = swh_var.isel({time_dim: 0})  # 첫 번째 시간 스텝

            # 위도/경도 좌표 이름 자동 감지 (ERA5는 latitude/longitude 또는 lat/lon)
            lat_dim = "latitude" if "latitude" in ds_wave.coords else "lat"
            lon_dim = "longitude" if "longitude" in ds_wave.coords else "lon"

            # swh가 NaN이 아닌 좌표 → 해양 격자 (lat, lon) DataFrame
            ocean_mask = swh_first.notnull().values        # 2D bool 배열 (lat × lon)
            lat_vals   = ds_wave.coords[lat_dim].values    # 위도 1D 배열
            lon_vals   = ds_wave.coords[lon_dim].values    # 경도 1D 배열

            # 2D bool 배열에서 True(해양)인 인덱스 추출
            lat_idx, lon_idx = np.where(ocean_mask)
            ocean_df = pd.DataFrame({
                "lat": lat_vals[lat_idx].astype("float32"),
                "lon": lon_vals[lon_idx].astype("float32"),
            })
            ds_wave.close()

            # wind DataFrame에서 해양 좌표만 남기기 (inner join = 교집합)
            # float32 정밀도 문제 방지를 위해 round 적용 (소수점 4자리)
            df["lat"] = df["lat"].astype("float32")
            df["lon"] = df["lon"].astype("float32")
            before_rows = len(df)
            df = df.merge(ocean_df, on=["lat", "lon"], how="inner")
            after_rows  = len(df)

            logger.info(
                f"  해양 필터링 완료: {before_rows:,} → {after_rows:,}행 "
                f"({before_rows - after_rows:,}행 육지 제거, "
                f"해양 비율 {after_rows / before_rows * 100:.1f}%)"
            )
        else:
            # wave 파일 없으면 필터링 건너뜀 (전 지구 격자 그대로 적재)
            # wave_path가 None(재분석 폴더 없음)이거나 파일이 존재하지 않는 경우 모두 해당
            wave_name = wave_path.name if wave_path is not None else "없음"
            logger.warning(
                f"  ⚠️  마스크 wave 파일 없음 → 해양 필터링 건너뜀: {wave_name}\n"
                f"      wind 전 지구 격자 {len(df):,}행 그대로 적재됩니다."
            )

    # ── 4단계: 필요한 컬럼만 선택 (없는 컬럼은 NaN으로 채움) ──
    for col in table_columns:
        if col not in df.columns:
            df[col] = np.nan
    df = df[table_columns]

    # ── 5단계: 시간 컬럼 타임존 처리 (모두 UTC) ──
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    if "issued_at" in df.columns:
        df["issued_at"] = pd.to_datetime(df["issued_at"], utc=True)

    total_rows = len(df)
    logger.info(f"  변환 완료: {total_rows:,}행 | 컬럼: {len(table_columns)}개")

    # ── 6단계: 파일 유형별 최적 적재 전략 선택 ──────────────────────────────
    #
    # 전략 A — 직접 COPY (wind / hycom / 예보):
    #   TimescaleDB hypertable에 직접 COPY → ON CONFLICT 오버헤드 없음 (가장 빠름)
    #   전제: 해당 날짜의 데이터가 아직 없는 최초 적재
    #
    # 전략 B — COPY → UPDATE (wave):
    #   1) tmp_load에 청크 COPY (인덱스 없음 → COPY 빠름)
    #   2) COPY 완료 후 tmp_load에 인덱스 생성
    #   3) main 테이블 UPDATE tmp_load WHERE PK 일치
    #   전제: 같은 날짜의 wind 행이 이미 있어야 함 (wind 먼저 적재 후 wave 적재)
    #
    # wave에서 INSERT ON CONFLICT DO UPDATE를 쓰지 않는 이유:
    #   TimescaleDB hypertable에 24.9M 행 INSERT ON CONFLICT는 10분 이상 소요됨
    #   반면 UPDATE는 인덱스 merge join으로 수 분 내 완료
    # ────────────────────────────────────────────────────────────────────────
    COPY_CHUNK_SIZE = 500_000   # COPY 청크 크기 (메모리 vs 속도 균형)

    # 적재 전략 결정
    #   전략 A: 직접 COPY (wind / hycom_current) — PK 충돌 없는 신규 데이터
    #   전략 B: tmp_load COPY → plain UPDATE (ecmwf_wave) — 기존 행의 wave 컬럼만 갱신
    #   전략 C: tmp_load COPY → INSERT ON CONFLICT DO UPDATE (예보 3종, Phase 14-B)
    #           예보는 같은 (datetime, lat, lon)에 매일 최신 issued_at으로 갱신 필요
    FORECAST_FILE_TYPES = {"ecmwf_fc_wind", "hycom_fc_current", "noaa_fc_wave"}
    use_update_strategy   = (file_type == "ecmwf_wave")       # 전략 B
    use_forecast_strategy = (file_type in FORECAST_FILE_TYPES) # 전략 C

    conn = get_connection()
    total_loaded = 0

    try:
        cursor = conn.cursor()
        col_str = ", ".join(table_columns)
        pk_str  = ", ".join(pk_cols)

        if not use_update_strategy and not use_forecast_strategy:
            # ── 전략 A: 직접 COPY to 메인 테이블 ──────────────────────────
            # COPY는 ON CONFLICT 없이 bulk insert → INSERT ON CONFLICT 대비 5~10배 빠름
            # 주의: 이미 해당 날짜 데이터가 있으면 PK 위반 오류 발생
            #   → 파이프라인은 pipeline_coverage 기반으로 재적재 방지
            num_chunks = (total_rows + COPY_CHUNK_SIZE - 1) // COPY_CHUNK_SIZE
            for chunk_idx in range(num_chunks):
                start = chunk_idx * COPY_CHUNK_SIZE
                end   = min(start + COPY_CHUNK_SIZE, total_rows)
                chunk_df = df.iloc[start:end]

                buffer = io.StringIO()
                chunk_df.to_csv(buffer, index=False, header=False, na_rep="\\N")
                buffer.seek(0)

                cursor.copy_expert(
                    f"COPY {target_table} ({col_str}) FROM STDIN "
                    f"WITH (FORMAT csv, NULL '\\N')",
                    buffer,
                )
                logger.debug(
                    f"  COPY 청크 {chunk_idx + 1}/{num_chunks} 완료 "
                    f"({start:,}~{end:,}행)"
                )
            total_loaded = total_rows

        elif use_update_strategy:
            # ── 전략 B: tmp_load COPY → main 테이블 UPDATE (wave 전용) ────
            # step 1) 임시 테이블 생성 (인덱스 없음 → COPY 빠름)
            col_defs = ", ".join(f"{c} {COLUMN_TYPES[c]}" for c in table_columns)
            cursor.execute(f"CREATE TEMP TABLE tmp_load ({col_defs})")

            # step 2) 청크 COPY → tmp_load
            num_chunks = (total_rows + COPY_CHUNK_SIZE - 1) // COPY_CHUNK_SIZE
            for chunk_idx in range(num_chunks):
                start = chunk_idx * COPY_CHUNK_SIZE
                end   = min(start + COPY_CHUNK_SIZE, total_rows)
                chunk_df = df.iloc[start:end]

                buffer = io.StringIO()
                chunk_df.to_csv(buffer, index=False, header=False, na_rep="\\N")
                buffer.seek(0)

                cursor.copy_expert(
                    f"COPY tmp_load ({col_str}) FROM STDIN "
                    f"WITH (FORMAT csv, NULL '\\N')",
                    buffer,
                )
                logger.debug(
                    f"  COPY 청크 {chunk_idx + 1}/{num_chunks} 완료 "
                    f"({start:,}~{end:,}행)"
                )

            # step 3) COPY 완료 후 tmp_load에 인덱스 생성
            #   인덱스가 있어야 UPDATE의 join이 hash/merge join으로 최적화됨
            logger.debug("  tmp_load 인덱스 생성 중...")
            cursor.execute(f"CREATE INDEX ON tmp_load ({pk_str})")
            logger.debug("  tmp_load 인덱스 생성 완료")

            # step 4) wind 행이 이미 있는 main 테이블에 wave 컬럼만 UPDATE
            #   INSERT ON CONFLICT DO UPDATE 대신 plain UPDATE 사용 (훨씬 빠름)
            #   wave update_cols: [swh, mwd, mwp, shts, mdts, mpts, shww, mdww, mpww]
            #
            #   work_mem 증가: 24.9M × 24.9M JOIN에 hash table ~600MB 필요
            #   기본값(64MB)이면 disk spill → 매우 느림 → 2GB로 늘려 메모리 내 처리
            cursor.execute("SET LOCAL work_mem = '2GB'")
            # PostgreSQL UPDATE SET 절에는 테이블 alias 불가 → 왼쪽에 컬럼명만 사용
            set_clause  = ", ".join(f"{c} = t.{c}" for c in update_cols)
            pk_where    = " AND ".join(f"r.{c} = t.{c}" for c in pk_cols)
            logger.debug("  wave UPDATE main 테이블 실행 중... (work_mem=2GB)")
            cursor.execute(f"""
                UPDATE {target_table} r
                SET    {set_clause}
                FROM   tmp_load t
                WHERE  {pk_where}
            """)
            total_loaded = cursor.rowcount

            # step 5) 임시 테이블 정리
            cursor.execute("DROP TABLE IF EXISTS tmp_load")

        else:
            # ── 전략 D: TRUNCATE → 직접 COPY (예보 전용) ────────────────────
            # 예보 데이터는 매일 전체 교체하는 특성:
            #   - "어제 예보"를 부분 갱신하는 것이 아니라 최신 예보로 완전 대체
            #   - UPSERT(전략 C)는 육지 격자 등 겹치지 않는 행이 그대로 잔존하는 문제 발생
            #   - TRUNCATE 후 COPY = 항상 깔끔한 최신 예보만 유지됨
            #
            # 전략 A(직접 COPY)와 동일하되, 적재 전 TRUNCATE가 추가됨.
            # UPSERT 오버헤드가 없으므로 전략 C보다 빠름.

            # step 1) 기존 예보 데이터 전체 삭제 (최신 예보로 완전 교체)
            logger.info(f"  예보 테이블 초기화 (TRUNCATE): {target_table}")
            cursor.execute(f"TRUNCATE TABLE {target_table}")

            # step 2) 직접 COPY (전략 A와 동일한 방식)
            num_chunks = (total_rows + COPY_CHUNK_SIZE - 1) // COPY_CHUNK_SIZE
            for chunk_idx in range(num_chunks):
                start = chunk_idx * COPY_CHUNK_SIZE
                end   = min(start + COPY_CHUNK_SIZE, total_rows)
                chunk_df = df.iloc[start:end]

                buffer = io.StringIO()
                chunk_df.to_csv(buffer, index=False, header=False, na_rep="\\N")
                buffer.seek(0)

                cursor.copy_expert(
                    f"COPY {target_table} ({col_str}) FROM STDIN "
                    f"WITH (FORMAT csv, NULL '\\N')",
                    buffer,
                )
                logger.debug(
                    f"  COPY 청크 {chunk_idx + 1}/{num_chunks} 완료 "
                    f"({start:,}~{end:,}행)"
                )
            total_loaded = total_rows

        # 커밋 1회
        conn.commit()
        cursor.close()

        logger.success(
            f"[적재 완료] {nc_path.name} | 총 {total_loaded:,}행 적재/갱신"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"[적재 실패] {nc_path.name}: {e}")
        raise

    finally:
        ds.close()     # NetCDF 파일 닫기 (메모리 해제)
        conn.close()   # DB 연결 닫기

    return total_loaded


def load_multiple_files(
    nc_paths: list[Path],
    table_name: str = None,
    batch_size: int = 50_000,
) -> dict:
    """
    여러 NetCDF 파일을 순서대로 DB에 적재

    Parameters
    ----------
    nc_paths   : list[Path]  →  적재할 파일 경로 목록
    table_name : str         →  적재할 테이블 이름 (None이면 파일명으로 자동 판단)
    batch_size : int         →  배치 크기

    Returns
    -------
    dict  →  {"success": 성공 파일 수, "failed": 실패 파일 수, "total_rows": 총 적재 행 수}
    """
    total_files   = len(nc_paths)
    success_count = 0
    failed_count  = 0
    total_rows    = 0

    logger.info(f"총 {total_files}개 파일 적재 시작")

    for idx, path in enumerate(nc_paths, start=1):
        logger.info(f"파일 {idx}/{total_files}: {path.name}")

        try:
            rows = load_netcdf_to_db(path, table_name, batch_size)
            total_rows    += rows
            success_count += 1

        except Exception as e:
            logger.error(f"파일 적재 실패 [{path.name}]: {e}")
            failed_count += 1
            # 한 파일 실패해도 나머지 계속 진행
            continue

    logger.info(
        f"전체 적재 완료 | 성공: {success_count}파일 | "
        f"실패: {failed_count}파일 | 총 {total_rows:,}행"
    )

    return {
        "success":    success_count,
        "failed":     failed_count,
        "total_rows": total_rows,
    }
