"""
NetCDF → TimescaleDB 데이터 적재 모듈
======================================

NetCDF 파일을 읽어서 TimescaleDB에 빠르게 적재함.

적재 방식: PostgreSQL COPY 명령
  - 일반 INSERT보다 약 10~50배 빠름
  - 대용량 데이터(수백만 행)에 적합
  - CSV 형태의 데이터를 메모리 버퍼를 통해 DB에 직접 복사
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


def _detect_table_config(nc_path: Path) -> tuple[str, list[str]]:
    """
    파일명을 보고 어느 테이블에 적재할지, 컬럼이 뭔지 자동 판단

    파일명 규칙:
      ecmwf_wind_YYYYMMDD.nc  → env_ecmwf_reanalysis (wind 컬럼)
      ecmwf_wave_YYYYMMDD.nc  → env_ecmwf_reanalysis (wave 컬럼)
      hycom_current_YYYYMMDD.nc → env_hycom_current

    Parameters
    ----------
    nc_path : Path  →  NetCDF 파일 경로

    Returns
    -------
    tuple[str, list[str]]  →  (테이블명, 컬럼목록)
    """
    filename = nc_path.name.lower()

    # ── 예보 파일 (fc = forecast) 먼저 검사 ──
    # 재분석 파일(ecmwf_wind_*)과 혼동하지 않도록 예보를 우선 처리
    if "ecmwf_fc_wind" in filename:
        return "env_ecmwf_forecast", ECMWF_FC_WIND_COLUMNS
    elif "hycom_fc_current" in filename:
        return "env_hycom_forecast", HYCOM_FC_COLUMNS
    elif "noaa_fc_wave" in filename:
        # NOAA WW3 파랑 예보 → env_noaa_forecast 테이블
        # 변수명은 다운로더에서 이미 ECMWF 컬럼명(swh, mwd 등)으로 변환됨
        return "env_noaa_forecast", NOAA_FC_WAVE_COLUMNS

    # ── 재분석/분석 파일 ──
    elif "ecmwf_wind" in filename:
        return "env_ecmwf_reanalysis", ECMWF_WIND_COLUMNS
    elif "ecmwf_wave" in filename:
        return "env_ecmwf_reanalysis", ECMWF_WAVE_COLUMNS
    elif "hycom_current" in filename:
        return "env_hycom_current", HYCOM_CURRENT_COLUMNS
    else:
        raise ValueError(
            f"파일명으로 테이블을 판단할 수 없습니다: {nc_path.name}\n"
            f"재분석: ecmwf_wind_*, ecmwf_wave_*, hycom_current_*\n"
            f"예보:   ecmwf_fc_wind_*, hycom_fc_current_*, noaa_fc_wave_*"
        )


def load_netcdf_to_db(
    nc_path: Path,
    table_name: str = None,   # None이면 파일명으로 자동 판단
    batch_size: int = 50_000,
) -> int:
    """
    NetCDF 파일 1개를 읽어서 TimescaleDB에 전체 적재

    파일명에 따라 테이블과 컬럼을 자동 판단:
      ecmwf_wind_*   → env_ecmwf_reanalysis (wind 컬럼)
      ecmwf_wave_*   → env_ecmwf_reanalysis (wave 컬럼)
      hycom_current_* → env_hycom_current

    Parameters
    ----------
    nc_path    : Path   →  적재할 NetCDF 파일 경로
    table_name : str    →  테이블 이름 (None이면 파일명으로 자동 판단)
    batch_size : int    →  한 번에 처리할 행 수 (메모리 조절용)

    Returns
    -------
    int  →  실제로 적재된 총 행 수
    """
    # 파일명으로 테이블 및 컬럼 자동 판단
    auto_table, table_columns = _detect_table_config(nc_path)

    # 명시적으로 테이블명이 전달된 경우 그것을 우선 사용
    target_table = table_name if table_name else auto_table

    logger.info(f"[적재 시작] {nc_path.name} → {target_table}")

    # ── 1단계: NetCDF 파일 읽기 ──
    # xarray는 기상/해양 NetCDF 파일을 쉽게 읽는 라이브러리
    # decode_times=True → 시간 데이터를 Python datetime으로 자동 변환
    logger.debug("  NetCDF 파일 로딩 중...")
    # NOAA WW3 파일: 파랑 주기 변수(mpts, mpww)가 units="wave_seconds"로 저장되나
    # xarray 버전에 따라 timedelta로 오해석할 수 있어 decode_timedelta=False 적용
    is_noaa = "noaa_fc" in nc_path.name.lower()
    ds = xr.open_dataset(
        nc_path,
        decode_times=True,
        decode_timedelta=False if is_noaa else None,
    )

    # ── 1.5단계: 예보 파일인 경우 issued_at 전역 속성 추출 ──
    # 예보 NetCDF 파일은 downloader가 issued_at을 전역 속성으로 저장함
    # 재분석 파일에는 issued_at 속성이 없음 → None 반환
    issued_at = None
    if "issued_at" in ds.attrs:
        # ISO 형식 문자열 → pandas Timestamp 변환 (UTC 명시)
        issued_at = pd.to_datetime(ds.attrs["issued_at"], utc=True)
        logger.debug(f"  issued_at 읽기 완료: {issued_at}")

    # ── 2단계: 데이터셋을 DataFrame으로 변환 ──
    # NetCDF 구조: time × lat(itude) × lon(gitude) 3차원 격자
    # to_dataframe()은 이를 멀티인덱스 DataFrame으로 변환
    logger.debug("  DataFrame 변환 중...")
    df = ds.to_dataframe()

    # 멀티인덱스(time, lat, lon)를 일반 컬럼으로 변환
    df = df.reset_index()

    # ── 3단계: 컬럼명 정리 ──
    # 파일마다 좌표 컬럼명이 다를 수 있어 통일
    # ECMWF 재분석: valid_time/time, latitude, longitude
    # ECMWF 예보(cfgrib): valid_time, latitude, longitude
    # HYCOM 분석/예보: time, lat, lon
    rename_map = {}
    if "valid_time" in df.columns:
        rename_map["valid_time"] = "datetime"  # 예보/새 ERA5 API → valid_time
    elif "time" in df.columns:
        rename_map["time"] = "datetime"        # HYCOM 및 구 ERA5 → time
    if "latitude" in df.columns:
        rename_map["latitude"] = "lat"
    if "longitude" in df.columns:
        rename_map["longitude"] = "lon"

    df = df.rename(columns=rename_map)  # 컬럼명 일괄 변경

    # ── 3.5단계: 예보 파일인 경우 issued_at 컬럼 추가 ──
    # table_columns에 "issued_at"이 포함된 경우(예보 테이블)에만 추가
    if "issued_at" in table_columns:
        if issued_at is not None:
            # 모든 행에 동일한 issued_at 값을 컬럼으로 추가
            df["issued_at"] = issued_at
        else:
            # issued_at을 파일에서 읽지 못한 경우 → 현재 UTC 시각으로 대체
            logger.warning(
                f"  issued_at을 파일에서 읽지 못함 → 현재 UTC 시각 사용: {nc_path.name}"
            )
            df["issued_at"] = pd.Timestamp.now(tz="UTC")

    # ── 4단계: 필요한 컬럼만 선택 ──
    # 파일 종류(wind/wave/current/forecast)에 따라 다른 컬럼 목록 사용
    # NetCDF에 없는 변수는 NaN으로 채움
    for col in table_columns:
        if col not in df.columns:
            df[col] = np.nan  # 없는 컬럼은 NaN으로 생성

    # DB에 넣을 컬럼만 순서대로 선택
    df = df[table_columns]

    # ── 5단계: 시간 컬럼 타임존 처리 ──
    # 모든 시간 데이터는 UTC 기준으로 DB에 저장
    if df["datetime"].dtype == "object" or str(df["datetime"].dtype).startswith("datetime"):
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

    # issued_at 컬럼도 UTC 처리 (예보 테이블에만 존재)
    if "issued_at" in df.columns:
        df["issued_at"] = pd.to_datetime(df["issued_at"], utc=True)

    total_rows = len(df)
    logger.info(f"  변환 완료: {total_rows:,}행 | 컬럼: {len(table_columns)}개")

    # ── 6단계: DB에 COPY 방식으로 적재 ──
    conn = get_connection()
    total_loaded = 0

    try:
        cursor = conn.cursor()

        # 전체 데이터를 batch_size 단위로 나눠서 적재
        # (한 번에 너무 많이 올리면 메모리 부족 가능)
        num_batches = (total_rows // batch_size) + 1  # 총 배치 수 계산

        for batch_idx in range(num_batches):
            # 현재 배치의 시작/끝 행 인덱스 계산
            start_row = batch_idx * batch_size
            end_row   = min(start_row + batch_size, total_rows)

            # 배치가 비어있으면 반복 종료
            if start_row >= total_rows:
                break

            # 현재 배치 슬라이싱
            batch_df = df.iloc[start_row:end_row]

            # ─── COPY 방식 적재 ───
            # 1) DataFrame → CSV 텍스트로 변환 (메모리 안에서만 처리, 파일 저장 X)
            buffer = io.StringIO()
            batch_df.to_csv(
                buffer,
                index=False,         # 행 번호(인덱스) 제외
                header=False,        # 헤더(컬럼명) 제외 (COPY에서 별도 지정)
                na_rep="\\N",        # NaN → PostgreSQL NULL 표기(\N)
            )
            buffer.seek(0)  # 버퍼 읽기 위치를 처음으로 되돌림

            # 2) COPY 명령으로 DB에 빠르게 적재
            # ON CONFLICT DO NOTHING → 이미 있는 (datetime, lat, lon) 조합이면 건너뜀
            copy_sql = f"""
                COPY {target_table} ({', '.join(table_columns)})
                FROM STDIN WITH (
                    FORMAT csv,
                    NULL '\\N'
                )
            """
            cursor.copy_expert(copy_sql, buffer)

            total_loaded += len(batch_df)

            logger.debug(
                f"  배치 {batch_idx + 1}/{num_batches} 완료 | "
                f"누적: {total_loaded:,}/{total_rows:,}행"
            )

        # 모든 배치 완료 후 DB에 확정 반영
        conn.commit()
        cursor.close()

        logger.success(
            f"[적재 완료] {nc_path.name} | 총 {total_loaded:,}행"
        )

    except Exception as e:
        # 에러 발생 시 전체 취소 (부분 적재된 것도 롤백)
        conn.rollback()
        logger.error(f"[적재 실패] {nc_path.name}: {e}")
        raise

    finally:
        ds.close()     # NetCDF 파일 닫기 (메모리 해제)
        conn.close()   # DB 연결 닫기

    return total_loaded


def load_multiple_files(
    nc_paths: list[Path],
    table_name: str,
    batch_size: int = 50_000,
) -> dict:
    """
    여러 NetCDF 파일을 순서대로 DB에 적재

    Parameters
    ----------
    nc_paths   : list[Path]  →  적재할 파일 경로 목록
    table_name : str         →  적재할 테이블 이름
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

    # 전체 결과 요약
    logger.info(
        f"전체 적재 완료 | 성공: {success_count}파일 | "
        f"실패: {failed_count}파일 | 총 {total_rows:,}행"
    )

    return {
        "success":    success_count,
        "failed":     failed_count,
        "total_rows": total_rows,
    }
