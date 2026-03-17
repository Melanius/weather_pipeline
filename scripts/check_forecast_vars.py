"""
예보 NetCDF 파일 변수 구조 검사 스크립트
==========================================

목적:
  forecast_download_only 모드로 다운로드한 .nc 파일을 열어
  어떤 변수가 실제로 채워졌는지 (NaN 비율 포함) 확인한다.

  ECMWF Open Data가 파랑 9개 변수 중 어디까지 제공하는지 파악하는 데 유용.

사용법:
  uv run python scripts/check_forecast_vars.py

  # 특정 파일 지정도 가능
  uv run python scripts/check_forecast_vars.py --file data/ecmwf/forecast/2026/03/ecmwf_fc_wave_20260316.nc

출력 예시:
  === ecmwf_fc_wind_20260316.nc ===
  issued_at : 2026-03-16T00:00:00
  시간 스텝 : 5개 (0h ~ 24h, 6h 간격)
  위도 범위 : -90.0 ~ 90.0  (721개 격자점)
  경도 범위 : -180.0 ~ 180.0 (1440개 격자점)
  ┌──────────┬───────────┬──────────┬───────────────────────────┐
  │ 변수     │ NaN 비율  │ 최솟값   │ 최댓값                    │
  ├──────────┼───────────┼──────────┼───────────────────────────┤
  │ u10      │   0.00%   │ -30.12   │ 28.45  ← ✅ 정상 제공     │
  │ v10      │   0.00%   │ -25.67   │ 32.11  ← ✅ 정상 제공     │
  └──────────┴───────────┴──────────┴───────────────────────────┘
"""

import argparse
from pathlib import Path

import numpy as np
import xarray as xr


# ─────────────────────────────────────────────────────
# 분석할 예보 파일 패턴
# ─────────────────────────────────────────────────────

# 검사 대상 파일 패턴 (파일명 포함 문자열 → 변수 목록)
# 파일을 열어서 이 변수들의 NaN 비율을 확인한다
FILE_PATTERNS = {
    "ecmwf_fc_wind":    ["u10", "v10"],
    # ecmwf_fc_wave 는 수집 중단 (2026-03-17) — 파랑은 noaa_fc_wave 에서 전담
    "hycom_fc_current": ["water_u", "water_v"],
    # NOAA WW3: 다운로더에서 변수명을 ECMWF 컬럼명으로 미리 변환하므로
    # 검사 시에도 swh/mwd/... 로 확인 (NOAA 원래 이름 Thgt/Tdir/... 아님)
    "noaa_fc_wave":     ["swh", "mwd", "mwp", "shts", "mdts", "mpts", "shww", "mdww", "mpww"],
}

# 변수별 설명 (출력 시 표시)
VAR_DESCRIPTIONS = {
    "u10":     "동서 풍속 (m/s)",
    "v10":     "남북 풍속 (m/s)",
    "swh":     "복합 유의파고 (m)",
    "mwd":     "평균 파랑 방향 (°)",
    "mwp":     "평균 파랑 주기 (s)",
    "shts":    "너울 유의파고 (m)",
    "mdts":    "너울 방향 (°)",
    "mpts":    "너울 주기 (s)",
    "shww":    "풍파 유의파고 (m)",
    "mdww":    "풍파 방향 (°)",
    "mpww":    "풍파 주기 (s)",
    "water_u": "동서 해류 속도 (m/s)",
    "water_v": "남북 해류 속도 (m/s)",
}


def _find_forecast_files(data_root: Path) -> list[Path]:
    """
    data/ 디렉토리에서 모든 예보 NetCDF 파일을 재귀 탐색

    탐색 경로:
      data/ecmwf/forecast/**/*.nc
      data/hycom/forecast/**/*.nc

    Parameters
    ----------
    data_root : Path  →  data/ 폴더 경로

    Returns
    -------
    list[Path]  →  발견된 예보 .nc 파일 목록 (정렬됨)
    """
    found = []

    # ECMWF 예보 폴더 탐색
    ecmwf_fc_dir = data_root / "ecmwf" / "forecast"
    if ecmwf_fc_dir.exists():
        found.extend(sorted(ecmwf_fc_dir.rglob("*.nc")))

    # HYCOM 예보 폴더 탐색
    hycom_fc_dir = data_root / "hycom" / "forecast"
    if hycom_fc_dir.exists():
        found.extend(sorted(hycom_fc_dir.rglob("*.nc")))

    # NOAA WW3 예보 폴더 탐색
    noaa_fc_dir = data_root / "noaa" / "forecast"
    if noaa_fc_dir.exists():
        found.extend(sorted(noaa_fc_dir.rglob("*.nc")))

    return found


def _get_expected_vars(filename: str) -> list[str]:
    """
    파일명을 보고 기대되는 변수 목록 반환

    Parameters
    ----------
    filename : str  →  파일명 (소문자)

    Returns
    -------
    list[str]  →  예상 변수 목록
    """
    for pattern, vars_list in FILE_PATTERNS.items():
        if pattern in filename.lower():
            return vars_list
    return []  # 알 수 없는 파일 → 빈 목록


def _inspect_time_axis(ds: xr.Dataset) -> tuple[int, str, str, str]:
    """
    Dataset의 시간 축 정보 추출

    Parameters
    ----------
    ds : xr.Dataset

    Returns
    -------
    tuple  →  (스텝 수, 첫 시각 문자열, 마지막 시각 문자열, 간격 추정 문자열)
    """
    # 시간 좌표 이름 탐색 (파일 종류마다 다를 수 있음)
    time_coord = None
    for name in ("valid_time", "time"):
        if name in ds.coords or name in ds.dims:
            time_coord = name
            break

    if time_coord is None:
        return 0, "?", "?", "?"

    time_vals = ds[time_coord].values  # numpy datetime64 배열

    # 스텝 수 및 시작/끝 시각
    n_steps  = len(time_vals)
    t_start  = str(time_vals[0])[:16]   # 초 이하 제거
    t_end    = str(time_vals[-1])[:16]

    # 간격 추정 (두 번째 - 첫 번째 스텝)
    if n_steps >= 2:
        delta_ns   = int(time_vals[1] - time_vals[0])  # 나노초 단위
        delta_h    = delta_ns // (3_600 * 10**9)        # 나노초 → 시간
        interval   = f"{delta_h}h 간격"
    else:
        interval = "-"

    return n_steps, t_start, t_end, interval


def _inspect_spatial_axes(ds: xr.Dataset) -> dict:
    """
    Dataset의 공간 축 정보 (위경도 범위, 격자점 수) 추출

    Parameters
    ----------
    ds : xr.Dataset

    Returns
    -------
    dict  →  {"lat": ..., "lon": ...} 정보
    """
    info = {}

    # 위도 (lat 또는 latitude)
    for lat_name in ("lat", "latitude"):
        if lat_name in ds.coords:
            lat_vals = ds[lat_name].values
            info["lat"] = {
                "name":  lat_name,
                "min":   float(lat_vals.min()),
                "max":   float(lat_vals.max()),
                "count": len(lat_vals),
            }
            break

    # 경도 (lon 또는 longitude)
    for lon_name in ("lon", "longitude"):
        if lon_name in ds.coords:
            lon_vals = ds[lon_name].values
            info["lon"] = {
                "name":  lon_name,
                "min":   float(lon_vals.min()),
                "max":   float(lon_vals.max()),
                "count": len(lon_vals),
            }
            break

    return info


def inspect_nc_file(nc_path: Path) -> None:
    """
    NetCDF 파일 1개를 열어 변수 구조 출력

    Parameters
    ----------
    nc_path : Path  →  검사할 NetCDF 파일 경로
    """
    print(f"\n{'=' * 60}")
    print(f"  파일: {nc_path.name}")
    print(f"  경로: {nc_path.parent}")
    print(f"{'=' * 60}")

    # ── 파일 존재 및 크기 확인 ──
    if not nc_path.exists():
        print(f"  [오류] 파일 없음: {nc_path}")
        return

    size_mb = nc_path.stat().st_size / (1024 * 1024)
    print(f"  파일 크기: {size_mb:.2f} MB")

    try:
        # NetCDF 파일 열기
        # NOAA WW3 파일은 파랑 주기 변수가 timedelta로 오해석될 수 있어
        # decode_timedelta=False 적용 (다른 파일에는 영향 없음)
        is_noaa = "noaa_fc" in nc_path.name.lower()
        ds = xr.open_dataset(
            nc_path,
            decode_times=True,
            decode_timedelta=False if is_noaa else None,
        )

        # ── 전역 속성 출력 (issued_at 등) ──
        print()
        print("  [전역 속성]")
        if ds.attrs:
            for key, val in ds.attrs.items():
                print(f"    {key}: {val}")
        else:
            print("    (없음)")

        # ── 시간 축 정보 ──
        n_steps, t_start, t_end, interval = _inspect_time_axis(ds)
        print()
        print("  [시간 축]")
        print(f"    스텝 수  : {n_steps}개")
        print(f"    시작     : {t_start} UTC")
        print(f"    끝       : {t_end} UTC")
        print(f"    간격     : {interval}")

        # ── 공간 축 정보 ──
        spatial = _inspect_spatial_axes(ds)
        print()
        print("  [공간 축]")
        if "lat" in spatial:
            la = spatial["lat"]
            print(f"    위도: {la['min']:.2f}° ~ {la['max']:.2f}° ({la['count']}개 격자점)")
        if "lon" in spatial:
            lo = spatial["lon"]
            print(f"    경도: {lo['min']:.2f}° ~ {lo['max']:.2f}° ({lo['count']}개 격자점)")

        # ── 변수별 NaN 비율 및 통계 ──
        expected_vars = _get_expected_vars(nc_path.name)

        print()
        print("  [변수 분석]")
        print(f"  {'변수':<10} {'제공여부':<8} {'NaN 비율':>9} {'최솟값':>10} {'최댓값':>10}  설명")
        print(f"  {'-'*10} {'-'*8} {'-'*9} {'-'*10} {'-'*10}  {'-'*20}")

        for var in expected_vars:
            # 변수 설명 (알 수 없는 변수는 빈 문자열)
            desc = VAR_DESCRIPTIONS.get(var, "")

            if var not in ds.data_vars:
                # 변수 자체가 파일에 없는 경우 (다운로드조차 안 된 것)
                print(f"  {var:<10} {'❌ 없음':<8} {'':>9} {'':>10} {'':>10}  {desc}")
                continue

            # 변수 데이터 로드 (전체 배열)
            data = ds[var].values.astype(float)
            total_vals = data.size              # 전체 원소 수
            nan_count  = int(np.isnan(data).sum())  # NaN 개수
            nan_ratio  = nan_count / total_vals * 100 if total_vals > 0 else 0

            if nan_ratio == 100.0:
                # 100% NaN → Open Data에서 미제공
                status = "⚠ 미제공"
                val_min_str = " " * 10
                val_max_str = " " * 10
            else:
                # 데이터 있음
                status    = "✅ 제공"
                val_min   = float(np.nanmin(data))
                val_max   = float(np.nanmax(data))
                val_min_str = f"{val_min:>10.3f}"
                val_max_str = f"{val_max:>10.3f}"

            print(
                f"  {var:<10} {status:<8} {nan_ratio:>8.1f}% "
                f"{val_min_str} {val_max_str}  {desc}"
            )

        # ── 파일에는 있지만 기대 목록에 없는 추가 변수 표시 ──
        extra_vars = [v for v in ds.data_vars if v not in expected_vars]
        if extra_vars:
            print()
            print(f"  [추가 변수 (기대 목록 외)] {extra_vars}")

        ds.close()

    except Exception as e:
        print(f"  [오류] 파일 읽기 실패: {e}")


def main():
    """메인 실행 함수"""

    # ── 커맨드라인 인수 파서 ──
    parser = argparse.ArgumentParser(
        description="예보 NetCDF 파일 변수 구조 검사",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 모든 예보 파일 검사 (data/ecmwf/forecast/, data/hycom/forecast/ 자동 탐색)
  uv run python scripts/check_forecast_vars.py

  # 특정 파일만 검사
  uv run python scripts/check_forecast_vars.py \\
      --file data/ecmwf/forecast/2026/03/ecmwf_fc_wave_20260316.nc
        """,
    )

    # --file 옵션: 특정 파일 지정 (없으면 전체 자동 탐색)
    parser.add_argument(
        "--file",
        type=Path,
        default=None,
        metavar="PATH",
        help="특정 .nc 파일 경로 지정 (없으면 data/ 폴더 자동 탐색)",
    )

    args = parser.parse_args()

    # 프로젝트 루트: 이 스크립트(scripts/)의 부모 폴더
    project_root = Path(__file__).parent.parent
    data_root    = project_root / "data"

    if args.file:
        # ── 특정 파일 지정된 경우 ──
        target = args.file if args.file.is_absolute() else project_root / args.file
        inspect_nc_file(target)

    else:
        # ── 자동 탐색: 예보 폴더에서 모든 .nc 파일 찾기 ──
        files = _find_forecast_files(data_root)

        if not files:
            print()
            print("[알림] 예보 파일이 없습니다.")
            print("  먼저 아래 명령으로 예보 데이터를 다운로드하세요:")
            print()
            print("  uv run python run.py --mode forecast_download_only --forecast-days 1")
            print()
            return

        print(f"\n총 {len(files)}개 예보 파일 발견")

        for nc_path in files:
            inspect_nc_file(nc_path)

    print()
    print("=" * 60)
    print("검사 완료")
    print("=" * 60)


if __name__ == "__main__":
    main()
