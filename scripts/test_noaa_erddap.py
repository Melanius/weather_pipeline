"""
NOAA ERDDAP URL 접근 테스트 스크립트
======================================

목적:
  NOAA WaveWatch III(WW3) 파랑 예보 데이터 소스가 현재도 살아있는지 확인하고,
  어떤 변수를 어떤 이름으로 제공하는지 파악한다.

테스트 대상 URL:
  1. coastwatch.pfeg.noaa.gov  — NOAA CoastWatch ERDDAP (이전 담당자 사용)
     - WW3 파랑: NWW3_Global_Best
     - GFS 바람: NCEP_Global_Best
  2. pae-paha.pacioos.hawaii.edu — PacIOOS ERDDAP
     - WW3 파랑: ww3_global

접근 방식:
  1단계: HTTP HEAD 요청으로 서버 생존 여부 확인 (데이터 다운로드 없이 빠름)
  2단계: OPeNDAP(.dds 엔드포인트)으로 변수 구조만 확인 (메타데이터만 수신)
  3단계: xarray로 소량 샘플 데이터 실제 로드 (변수값 범위 확인)

사용법:
  uv run python scripts/test_noaa_erddap.py

출력 예시:
  [서버 1] coastwatch.pfeg.noaa.gov
    NWW3_Global_Best  → HTTP 200 ✅
    변수 목록: ['Thgt', 'Tper', 'Tdir', 'shgt', 'sper', 'sdir', 'whgt', 'wper', 'wdir']
    시간 범위: 2026-03-16T00:00 ~ 2026-03-18T00:00 (예보 48h)
"""

import sys
import requests   # HTTP 상태 확인용
import xarray as xr  # OPeNDAP / NetCDF 데이터 열기
import numpy as np
from datetime import datetime, timedelta, timezone


# ─────────────────────────────────────────────────────
# 테스트 대상 URL 목록
# ─────────────────────────────────────────────────────

# 각 항목: (설명, 데이터셋 기본 URL, 기대 변수 목록)
ERDDAP_TARGETS = [
    {
        "label":     "CoastWatch NWW3 파랑 (이전 담당자 사용)",
        "server":    "coastwatch.pfeg.noaa.gov",
        "dataset_id": "NWW3_Global_Best",
        "base_url":  "https://coastwatch.pfeg.noaa.gov/erddap/griddap/NWW3_Global_Best",
        # ERDDAP info 페이지 (변수 목록 HTML)
        "info_url":  "https://coastwatch.pfeg.noaa.gov/erddap/info/NWW3_Global_Best/index.json",
        # OPeNDAP URL (xarray로 열기, 데이터 구조 확인)
        "opendap_url": "https://coastwatch.pfeg.noaa.gov/erddap/griddap/NWW3_Global_Best",
        # 이전 담당자가 사용한 변수명
        "expected_vars": ["Tdir", "Tper", "Thgt", "sdir", "sper", "shgt", "wdir", "wper", "whgt"],
        "type": "wave",
    },
    {
        "label":     "CoastWatch NCEP GFS 바람 (이전 담당자 사용)",
        "server":    "coastwatch.pfeg.noaa.gov",
        "dataset_id": "NCEP_Global_Best",
        "base_url":  "https://coastwatch.pfeg.noaa.gov/erddap/griddap/NCEP_Global_Best",
        "info_url":  "https://coastwatch.pfeg.noaa.gov/erddap/info/NCEP_Global_Best/index.json",
        "opendap_url": "https://coastwatch.pfeg.noaa.gov/erddap/griddap/NCEP_Global_Best",
        "expected_vars": ["ugrd10m", "vgrd10m"],
        "type": "wind",
    },
    {
        "label":     "PacIOOS WW3 파랑 (forecast_wave.py 사용)",
        "server":    "pae-paha.pacioos.hawaii.edu",
        "dataset_id": "ww3_global",
        "base_url":  "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/ww3_global",
        "info_url":  "https://pae-paha.pacioos.hawaii.edu/erddap/info/ww3_global/index.json",
        "opendap_url": "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/ww3_global",
        "expected_vars": ["Tdir", "Tper", "Thgt", "sdir", "sper", "shgt", "wdir", "wper", "whgt"],
        "type": "wave",
    },
]


# ─────────────────────────────────────────────────────
# STEP 1: HTTP 서버 생존 확인
# ─────────────────────────────────────────────────────

def check_http_status(info_url: str, timeout: int = 15) -> tuple[bool, int | None, str]:
    """
    ERDDAP info JSON URL에 HTTP GET 요청으로 서버 생존 확인

    Parameters
    ----------
    info_url : str   →  ERDDAP /info/{dataset_id}/index.json 경로
    timeout  : int   →  대기 시간 (초)

    Returns
    -------
    tuple  →  (성공여부, HTTP 상태코드, 메시지)
    """
    try:
        # GET 요청 (HEAD는 일부 ERDDAP에서 405 반환하므로 GET 사용)
        resp = requests.get(info_url, timeout=timeout)
        if resp.status_code == 200:
            return True, 200, "✅ 서버 응답 정상"
        elif resp.status_code == 404:
            return False, 404, "❌ 데이터셋 없음 (URL 변경됐을 수 있음)"
        else:
            return False, resp.status_code, f"⚠ 예상 못한 응답 코드"
    except requests.exceptions.ConnectionError:
        return False, None, "❌ 서버 연결 실패 (서비스 종료 또는 도메인 변경)"
    except requests.exceptions.Timeout:
        return False, None, f"⚠ 응답 시간 초과 ({timeout}초)"
    except Exception as e:
        return False, None, f"❌ 오류: {e}"


# ─────────────────────────────────────────────────────
# STEP 2: ERDDAP Info JSON 파싱 — 변수 목록 확인
# ─────────────────────────────────────────────────────

def get_variable_list(info_url: str, timeout: int = 15) -> list[dict]:
    """
    ERDDAP /info/{dataset_id}/index.json 에서 변수 목록 파싱

    ERDDAP info JSON 구조:
      { "table": { "columnNames": [...], "rows": [ [type, var_name, attr, value], ... ] } }
    type 이 "variable" 인 행에서 변수명을 추출

    Returns
    -------
    list[dict]  →  [{"name": "Thgt", "unit": "m", "long_name": "Wave Height"}, ...]
    """
    try:
        resp = requests.get(info_url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        # columnNames에서 각 컬럼의 인덱스 파악
        cols = data["table"]["columnNames"]
        rows = data["table"]["rows"]

        # 컬럼 인덱스 매핑
        row_type_idx    = cols.index("Row Type")       if "Row Type"    in cols else 0
        var_name_idx    = cols.index("Variable Name")  if "Variable Name" in cols else 1
        attr_name_idx   = cols.index("Attribute Name") if "Attribute Name" in cols else 2
        value_idx       = cols.index("Value")          if "Value"        in cols else 4

        # 변수별 속성 수집
        variables = {}
        for row in rows:
            row_type = row[row_type_idx]
            var_name = row[var_name_idx]
            attr     = row[attr_name_idx]
            value    = row[value_idx]

            if row_type == "variable":
                if var_name not in variables:
                    variables[var_name] = {"name": var_name, "units": "", "long_name": ""}
                if attr == "units":
                    variables[var_name]["units"] = value
                elif attr == "long_name":
                    variables[var_name]["long_name"] = value

        return list(variables.values())

    except Exception as e:
        return []


# ─────────────────────────────────────────────────────
# STEP 3: OPeNDAP으로 소량 샘플 데이터 로드
# ─────────────────────────────────────────────────────

def load_sample_data(opendap_url: str, expected_vars: list[str]) -> dict:
    """
    OPeNDAP URL로 데이터셋을 열어 시간 범위 및 변수 존재 여부 확인

    실제 데이터를 다운로드하지 않고 좌표/메타데이터만 확인.
    (xr.open_dataset은 처음엔 메타데이터만 가져오므로 빠름)

    Parameters
    ----------
    opendap_url   : str         →  ERDDAP griddap URL
    expected_vars : list[str]   →  확인할 변수명 목록

    Returns
    -------
    dict  →  {
        "success": bool,
        "time_start": str,  # 데이터 시작 시각
        "time_end":   str,  # 데이터 끝 시각
        "time_count": int,  # 시간 스텝 수
        "vars_found": list[str],   # 실제 존재하는 변수
        "vars_missing": list[str], # 없는 변수
        "error": str | None,
    }
    """
    result = {
        "success":      False,
        "time_start":   "?",
        "time_end":     "?",
        "time_count":   0,
        "vars_found":   [],
        "vars_missing": [],
        "error":        None,
    }

    try:
        # OPeNDAP URL로 원격 데이터셋 열기 (메타데이터만)
        # ERDDAP griddap은 OPeNDAP 호환이므로 xarray로 직접 접근 가능
        print(f"    [OPeNDAP 연결 중...] {opendap_url}")
        ds = xr.open_dataset(opendap_url)

        # 시간 좌표 확인
        time_coord = None
        for t_name in ("time", "TIME"):
            if t_name in ds.coords or t_name in ds.dims:
                time_coord = t_name
                break

        if time_coord is not None:
            t_vals = ds[time_coord].values
            result["time_count"] = len(t_vals)
            result["time_start"] = str(t_vals[0])[:16]
            result["time_end"]   = str(t_vals[-1])[:16]

        # 변수 존재 여부 확인
        for var in expected_vars:
            if var in ds.data_vars:
                result["vars_found"].append(var)
            else:
                result["vars_missing"].append(var)

        ds.close()
        result["success"] = True

    except Exception as e:
        result["error"] = str(e)

    return result


# ─────────────────────────────────────────────────────
# 출력 헬퍼
# ─────────────────────────────────────────────────────

def print_separator(char: str = "─", width: int = 62) -> None:
    """구분선 출력"""
    print(char * width)


def print_variable_table(var_list: list[dict], expected_vars: list[str]) -> None:
    """
    변수 목록을 표 형식으로 출력
    기대 변수(expected_vars)와 실제 목록을 비교해 ✅/❌ 표시
    """
    if not var_list:
        print("    (변수 목록 파싱 실패)")
        return

    # 기대 변수 존재 여부 표시
    expected_set = set(expected_vars)
    # 실제 변수 중 기대 변수만 필터
    relevant = [v for v in var_list if v["name"] in expected_set]
    others   = [v for v in var_list if v["name"] not in expected_set]

    if relevant:
        print(f"  {'변수명':<12} {'단위':<8}  {'설명'}")
        print(f"  {'─'*12} {'─'*8}  {'─'*30}")
        for v in relevant:
            check = "✅"
            print(f"  {v['name']:<12} {v['units']:<8}  {v['long_name']}")
    if others:
        other_names = [v["name"] for v in others[:8]]  # 최대 8개만 표시
        extra = f" +{len(others)-8}개" if len(others) > 8 else ""
        print(f"  [기타 변수] {other_names}{extra}")


# ─────────────────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────────────────

def main() -> None:
    """NOAA ERDDAP 전체 테스트 실행"""

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print()
    print_separator("═")
    print(f"  NOAA ERDDAP URL 접근 테스트")
    print(f"  실행 시각: {now_str}")
    print_separator("═")

    # 최종 결과 요약용
    summary = []

    for i, target in enumerate(ERDDAP_TARGETS, start=1):

        print()
        print_separator()
        print(f"  [{i}] {target['label']}")
        print(f"      서버: {target['server']}")
        print(f"      데이터셋: {target['dataset_id']}")
        print_separator()

        # ── STEP 1: HTTP 상태 확인 ──
        print("  [1단계] HTTP 서버 생존 확인...")
        ok, code, msg = check_http_status(target["info_url"])
        print(f"    상태: {msg} (HTTP {code})")

        if not ok:
            summary.append({
                "label":   target["label"],
                "status":  "❌ 접근 불가",
                "vars_ok": False,
            })
            continue  # 서버 없으면 나머지 단계 건너뜀

        # ── STEP 2: 변수 목록 확인 ──
        print()
        print("  [2단계] 제공 변수 목록 확인...")
        var_list = get_variable_list(target["info_url"])

        if var_list:
            print(f"    총 {len(var_list)}개 변수 발견")
            # 기대 변수와 비교
            var_names = {v["name"] for v in var_list}
            found   = [v for v in target["expected_vars"] if v in var_names]
            missing = [v for v in target["expected_vars"] if v not in var_names]

            print()
            print("  [기대 변수 확인]")
            print_variable_table(var_list, target["expected_vars"])

            if missing:
                print()
                print(f"  ⚠ 기대 변수 중 없는 항목: {missing}")
        else:
            found   = []
            missing = target["expected_vars"][:]
            print("    ⚠ 변수 목록 파싱 실패 (ERDDAP JSON 구조 변경 가능성)")

        # ── STEP 3: OPeNDAP 소량 샘플 로드 ──
        print()
        print("  [3단계] OPeNDAP 실제 접근 테스트...")
        sample = load_sample_data(target["opendap_url"], target["expected_vars"])

        if sample["success"]:
            print(f"    ✅ OPeNDAP 연결 성공")
            print(f"    시간 범위: {sample['time_start']} ~ {sample['time_end']}")
            print(f"    시간 스텝: {sample['time_count']}개")
            if sample["vars_found"]:
                print(f"    변수 확인: ✅ {sample['vars_found']}")
            if sample["vars_missing"]:
                print(f"    변수 없음: ❌ {sample['vars_missing']}")
        else:
            print(f"    ❌ OPeNDAP 연결 실패: {sample['error']}")

        # 요약 기록
        vars_complete = (len(missing) == 0 and sample["success"])
        summary.append({
            "label":    target["label"],
            "status":   "✅ 접근 가능" if sample["success"] else "⚠ HTTP OK / OPeNDAP 실패",
            "vars_ok":  vars_complete,
            "found":    found,
            "missing":  missing,
        })

    # ── 최종 요약 ──
    print()
    print_separator("═")
    print("  최종 요약")
    print_separator("═")
    print(f"  {'데이터셋':<40} {'서버':<15} {'변수 완전성'}")
    print(f"  {'─'*40} {'─'*15} {'─'*10}")
    for s in summary:
        vars_mark = "✅ 완전" if s.get("vars_ok") else "⚠ 불완전"
        print(f"  {s['label']:<40} {s['status']:<15} {vars_mark}")

    print()
    print_separator("═")
    print("  [다음 단계 안내]")
    print_separator("═")
    # 결과에 따른 권고 출력
    accessible = [s for s in summary if "✅" in s["status"]]
    if accessible:
        print("  ✅ 접근 가능한 URL이 있습니다.")
        print("  → NOAA WW3 수집 코드 개발 진행 가능")
        print("  → DB 스키마에 env_noaa_forecast 테이블 추가 후 개발")
    else:
        print("  ❌ 접근 가능한 URL이 없습니다.")
        print("  → NOAA ERDDAP 서비스 변경 확인 필요")
        print("  → 대안: ECMWF wave 3개로 진행 or 다른 소스 탐색")
    print()


if __name__ == "__main__":
    main()
