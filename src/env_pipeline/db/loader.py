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
TABLE_PK = {
    "env_ecmwf_reanalysis": ["datetime", "lat", "lon"],
    "env_hycom_current":    ["datetime", "lat", "lon"],
    "env_ecmwf_forecast":   ["issued_at", "datetime", "lat", "lon"],
    "env_hycom_forecast":   ["issued_at", "datetime", "lat", "lon"],
    "env_noaa_forecast":    ["issued_at", "datetime", "lat", "lon"],
}

# 파일 유형별 ON CONFLICT 시 갱신할 컬럼 목록
# None → ON CONFLICT DO NOTHING (예보 파일: 동일 발행일 중복 무시)
# list → ON CONFLICT DO UPDATE SET (해당 컬럼만 갱신)
UPSERT_UPDATE_COLS = {
    "ecmwf_wind":    ["u10", "v10"],                              # 재분석 wind: wind 컬럼만 갱신
    "ecmwf_wave":    ["swh", "mwd", "mwp",                       # 재분석 wave: wave 컬럼만 갱신
                      "shts", "mdts", "mpts",
                      "shww", "mdww", "mpww"],
    "hycom_current": ["water_u", "water_v"],                      # HYCOM 분석: 해류 컬럼만 갱신
    "ecmwf_fc_wind": None,                                        # 예보: DO NOTHING
    "hycom_fc_current": None,
    "noaa_fc_wave":  None,
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
    if "ecmwf_fc_wind" in filename:
        return "env_ecmwf_forecast", ECMWF_FC_WIND_COLUMNS, "ecmwf_fc_wind", None
    elif "hycom_fc_current" in filename:
        return "env_hycom_forecast", HYCOM_FC_COLUMNS, "hycom_fc_current", None
    elif "noaa_fc_wave" in filename:
        return "env_noaa_forecast", NOAA_FC_WAVE_COLUMNS, "noaa_fc_wave", None

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

    # wave 파일 여부 판단: wind/hycom/예보는 직접 COPY, wave는 UPDATE 전략
    use_update_strategy = (file_type == "ecmwf_wave")

    conn = get_connection()
    total_loaded = 0

    try:
        cursor = conn.cursor()
        col_str = ", ".join(table_columns)
        pk_str  = ", ".join(pk_cols)

        if not use_update_strategy:
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

        else:
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
