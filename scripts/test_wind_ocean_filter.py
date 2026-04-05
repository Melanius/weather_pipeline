"""
Wind 해양 격자 필터링 테스트 스크립트
=======================================
DB 연결 없이 NetCDF 파일 로딩 → 필터링 → 결과 확인만 수행.

실행:
  uv run python scripts/test_wind_ocean_filter.py
  uv run python scripts/test_wind_ocean_filter.py --date 20260306  # 날짜 지정
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


def test_filter(date_str: str = "20260305") -> None:
    """
    지정 날짜의 wind 파일에 해양 필터링을 적용하고 결과를 출력.
    DB 연결 없이 로컬 NC 파일만 사용.
    """
    # ── 파일 경로 구성 ──
    year  = date_str[:4]
    month = date_str[4:6]
    base_dir = Path(__file__).parent.parent / "data" / "ecmwf" / "reanalysis" / year / month

    wind_path = base_dir / f"ecmwf_wind_{date_str}.nc"
    wave_path = base_dir / f"ecmwf_wave_{date_str}.nc"

    print(f"\n{'='*55}")
    print(f"  Wind 해양 필터링 테스트 — {date_str}")
    print(f"{'='*55}")
    print(f"  Wind 파일: {wind_path.name}  {'✅ 존재' if wind_path.exists() else '❌ 없음'}")
    print(f"  Wave 파일: {wave_path.name}  {'✅ 존재' if wave_path.exists() else '❌ 없음'}")

    if not wind_path.exists():
        print("\n[오류] wind 파일이 없습니다.")
        return
    if not wave_path.exists():
        print("\n[경고] wave 파일이 없습니다 → 필터링 건너뜀 테스트 불가")
        return

    # ── 1단계: wind 파일 로딩 ──
    print("\n[1단계] wind NetCDF 로딩 중...")
    ds_wind = xr.open_dataset(wind_path, decode_times=True)

    # DataFrame 변환
    df = ds_wind.to_dataframe().reset_index()
    ds_wind.close()

    # 컬럼명 통일 (latitude/longitude → lat/lon)
    rename_map = {}
    if "valid_time"  in df.columns: rename_map["valid_time"]  = "datetime"
    if "time"        in df.columns: rename_map["time"]        = "datetime"
    if "latitude"    in df.columns: rename_map["latitude"]    = "lat"
    if "longitude"   in df.columns: rename_map["longitude"]   = "lon"
    df = df.rename(columns=rename_map)

    before_rows = len(df)
    print(f"  → 전체 격자(육지+바다): {before_rows:,}행")
    print(f"  → 컬럼 목록: {list(df.columns)}")

    # ── 2단계: wave 파일에서 해양 마스크 추출 ──
    print("\n[2단계] wave 파일에서 해양 격자 마스크 추출 중...")
    ds_wave = xr.open_dataset(wave_path, decode_times=True)

    swh_var  = ds_wave["swh"]
    time_dim = swh_var.dims[0]                        # 시간 차원 이름
    swh_first = swh_var.isel({time_dim: 0})           # 첫 번째 시간 스텝

    # 위도/경도 좌표 이름 감지
    lat_dim = "latitude" if "latitude" in ds_wave.coords else "lat"
    lon_dim = "longitude" if "longitude" in ds_wave.coords else "lon"

    # swh가 NaN 아닌 격자 = 해양
    ocean_mask = swh_first.notnull().values            # 2D bool 배열
    lat_vals   = ds_wave.coords[lat_dim].values        # 위도 배열
    lon_vals   = ds_wave.coords[lon_dim].values        # 경도 배열

    lat_idx, lon_idx = np.where(ocean_mask)
    ocean_df = pd.DataFrame({
        "lat": lat_vals[lat_idx].astype("float32"),
        "lon": lon_vals[lon_idx].astype("float32"),
    })
    ds_wave.close()

    ocean_grid_count = len(ocean_df)                   # 해양 격자 수 (시간 1스텝 기준)
    total_grid       = swh_first.size                  # 전체 격자 수
    print(f"  → 전체 격자: {total_grid:,}개")
    print(f"  → 해양 격자: {ocean_grid_count:,}개 ({ocean_grid_count / total_grid * 100:.1f}%)")
    print(f"  → 육지 격자: {total_grid - ocean_grid_count:,}개 ({(total_grid - ocean_grid_count) / total_grid * 100:.1f}%)")

    # ── 3단계: wind DataFrame 필터링 ──
    print("\n[3단계] wind DataFrame 필터링 적용 중...")
    df["lat"] = df["lat"].astype("float32")
    df["lon"] = df["lon"].astype("float32")

    df_filtered = df.merge(ocean_df, on=["lat", "lon"], how="inner")
    after_rows  = len(df_filtered)

    # ── 4단계: 결과 출력 ──
    print(f"\n{'='*55}")
    print(f"  [결과 요약]")
    print(f"{'='*55}")
    print(f"  필터링 전: {before_rows:,}행")
    print(f"  필터링 후: {after_rows:,}행")
    print(f"  제거된 행: {before_rows - after_rows:,}행 (육지 격자)")
    print(f"  해양 비율: {after_rows / before_rows * 100:.1f}%")
    print(f"  행수 감소: {(before_rows - after_rows) / before_rows * 100:.1f}%")

    # NaN 비율 확인 (필터링 후 u10/v10에 NaN이 없어야 정상)
    u10_nan = df_filtered["u10"].isna().sum()
    v10_nan = df_filtered["v10"].isna().sum()
    print(f"\n  u10 NaN 수: {u10_nan:,}  ({'✅ 정상' if u10_nan == 0 else '⚠️ NaN 존재'})")
    print(f"  v10 NaN 수: {v10_nan:,}  ({'✅ 정상' if v10_nan == 0 else '⚠️ NaN 존재'})")

    # 필터링된 데이터 샘플 출력
    print(f"\n  [첫 5행 샘플]")
    print(df_filtered[["datetime", "lat", "lon", "u10", "v10"]].head())
    print(f"{'='*55}\n")


def main():
    parser = argparse.ArgumentParser(description="Wind 해양 필터링 테스트")
    parser.add_argument(
        "--date",
        default="20260305",
        metavar="YYYYMMDD",
        help="테스트할 날짜 (기본값: 20260305)",
    )
    args = parser.parse_args()
    test_filter(args.date)


if __name__ == "__main__":
    main()
