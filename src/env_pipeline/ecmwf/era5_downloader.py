"""
ECMWF ERA5 재분석 데이터 다운로드 모듈
======================================

동작 방식:
  - config/down.json 에서 시작일/종료일을 읽어 날짜 범위 결정
  - 하루 1파일씩, wind와 wave를 별도 파일로 다운로드
  - 저장 경로: data/ecmwf/reanalysis/YYYY/MM/ecmwf_{wind|wave}_YYYYMMDD.nc
  - 이미 파일이 있으면 건너뜀 (중단 후 재시작 안전)

파일 명명 규칙:
  ecmwf_wind_20250214.nc  ← 바람 2변수, 해당일 24시간
  ecmwf_wave_20250214.nc  ← 파랑 9변수, 해당일 24시간
"""

import os
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import cdsapi
import xarray as xr  # 경도 변환을 위한 NetCDF 처리 라이브러리
from loguru import logger


# ─────────────────────────────────────────────
# 수집 변수 정의
# ─────────────────────────────────────────────

# 바람(Wind) 변수: 2개
WIND_VARIABLES = [
    "10m_u_component_of_wind",   # u10: 동서 방향 풍속 (m/s, 동쪽 +)
    "10m_v_component_of_wind",   # v10: 남북 방향 풍속 (m/s, 북쪽 +)
]

# 파랑(Wave) 변수: 9개 (기획서 전체)
WAVE_VARIABLES = [
    "significant_height_of_combined_wind_waves_and_swell",  # swh: 복합 유의파고 (m)
    "mean_wave_direction",                                   # mwd: 평균 파랑 방향 (°)
    "mean_wave_period",                                      # mwp: 평균 파랑 주기 (s)
    "significant_height_of_total_swell",                     # shts: 너울 유의파고 (m)
    "mean_direction_of_total_swell",                         # mdts: 너울 방향 (°)
    "mean_period_of_total_swell",                            # mpts: 너울 주기 (s)
    "significant_height_of_wind_waves",                      # shww: 풍파 유의파고 (m)
    "mean_direction_of_wind_waves",                          # mdww: 풍파 방향 (°)
    "mean_period_of_wind_waves",                             # mpww: 풍파 주기 (s)
]

# data_type 문자열 → 변수 목록 매핑
VARIABLES_MAP = {
    "wind": WIND_VARIABLES,
    "wave": WAVE_VARIABLES,
}


def load_date_range(json_path: Path) -> tuple[datetime, datetime]:
    """
    config/down.json 에서 시작일/종료일을 읽어 반환

    down.json 형식:
        {
            "start_date": "2025-01-14",
            "end_date":   "2025-02-14"
        }

    Parameters
    ----------
    json_path : Path  →  down.json 파일 경로

    Returns
    -------
    tuple[datetime, datetime]  →  (시작일, 종료일) UTC 자정 기준
    """
    # JSON 파일 존재 여부 확인
    if not json_path.exists():
        raise FileNotFoundError(
            f"날짜 설정 파일이 없습니다: {json_path}\n"
            f"config/down.json 파일에 start_date, end_date를 입력해주세요."
        )

    # JSON 파일 읽기
    with open(json_path, encoding="utf-8") as f:
        config = json.load(f)

    # 날짜 문자열 → datetime 변환 (UTC 타임존 적용)
    start_date = datetime.strptime(config["start_date"], "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    end_date = datetime.strptime(config["end_date"], "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )

    # 날짜 범위 유효성 검사
    if start_date > end_date:
        raise ValueError(
            f"start_date({config['start_date']})가 "
            f"end_date({config['end_date']})보다 늦습니다."
        )

    logger.info(
        f"날짜 범위 로드: {config['start_date']} ~ {config['end_date']} "
        f"({(end_date - start_date).days + 1}일)"
    )
    return start_date, end_date


class ERA5Downloader:
    """
    ECMWF ERA5 재분석 데이터 다운로더

    - wind와 wave를 별도 파일로 하루 1파일씩 다운로드
    - 날짜 범위는 config/down.json 에서 읽어옴

    사용 예:
        downloader = ERA5Downloader(output_dir=Path("data/ecmwf/reanalysis"))
        downloader.run(json_path=Path("config/down.json"), resolution=1.0)
    """

    # CDS API 데이터셋 이름 (ERA5 단일 레벨 변수 모음)
    DATASET = "reanalysis-era5-single-levels"

    def __init__(self, output_dir: Path):
        """
        Parameters
        ----------
        output_dir : Path  →  NetCDF 파일을 저장할 최상위 디렉토리
        """
        # CDS API 클라이언트 초기화
        # .env 에서 읽은 환경변수로 인증
        self.client = cdsapi.Client(
            url=os.environ["CDS_API_URL"],
            key=os.environ["CDS_API_KEY"],
            quiet=True,  # cdsapi 자체 로그 숨김
        )

        self.output_dir = output_dir

        # 저장 폴더가 없으면 자동 생성
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"ERA5Downloader 준비 완료 | 저장 경로: {self.output_dir}")

    def _get_output_path(self, date: datetime, data_type: str) -> Path:
        """
        저장 파일 경로 생성

        경로 예: data/ecmwf/reanalysis/2025/02/ecmwf_wave_20250214.nc

        Parameters
        ----------
        date      : datetime  →  대상 날짜
        data_type : str       →  "wind" 또는 "wave"
        """
        # 연/월 폴더 생성 (없으면 자동 생성)
        folder = self.output_dir / f"{date.year:04d}" / f"{date.month:02d}"
        folder.mkdir(parents=True, exist_ok=True)

        # 파일명: ecmwf_wind_YYYYMMDD.nc 또는 ecmwf_wave_YYYYMMDD.nc
        filename = f"ecmwf_{data_type}_{date.strftime('%Y%m%d')}.nc"
        return folder / filename

    def _build_request(self, date: datetime, data_type: str, resolution: float) -> dict:
        """
        CDS API 요청 파라미터 생성

        Parameters
        ----------
        date       : datetime  →  대상 날짜
        data_type  : str       →  "wind" 또는 "wave"
        resolution : float     →  공간 해상도 (0.25 / 0.5 / 1.0)
        """
        # 해당 data_type의 변수 목록 선택
        variables = VARIABLES_MAP[data_type]

        # 00:00 ~ 23:00 (24개 시간대) 문자열 리스트 생성
        hours = [f"{h:02d}:00" for h in range(24)]

        return {
            "product_type":    ["reanalysis"],      # 재분석 데이터 타입
            "variable":        variables,            # wind 또는 wave 변수 목록
            "year":            str(date.year),       # 연도 문자열
            "month":           f"{date.month:02d}",  # 월 (2자리)
            "day":             [f"{date.day:02d}"],  # 일 (리스트 형태로 전달)
            "time":            hours,                # 24개 시간대 전체
            "grid":            [resolution, resolution],  # 공간 해상도
            "data_format":     "netcdf",             # 출력 형식
            "download_format": "unarchived",         # 압축 없이 단일 파일
        }

    def _fix_longitude(self, nc_path: Path) -> None:
        """
        NetCDF 파일의 경도를 0~360 → -180~180으로 변환하여 덮어씀

        CDS API는 경도를 0~359.75 범위로 반환함.
        HYCOM은 -180~180 기준이므로 ECMWF 재분석도 동일 좌표계로 맞춰야
        DB에서 JOIN 및 위치 조회 시 일관성이 유지됨.

        이미 -180~180 범위인 파일은 변환 없이 그대로 둠.

        Parameters
        ----------
        nc_path : Path  →  변환할 NetCDF 파일 경로 (in-place 덮어씀)
        """
        ds = xr.open_dataset(nc_path)

        # 경도 좌표 이름 탐색 (longitude 또는 lon)
        lon_coord = None
        for name in ("longitude", "lon"):
            if name in ds.coords:
                lon_coord = name
                break

        # 경도 최댓값이 180 초과인 경우에만 변환 (이미 변환된 파일 건너뜀)
        if lon_coord is None or float(ds[lon_coord].values.max()) <= 180:
            ds.close()
            return

        # (lon + 180) % 360 - 180 공식으로 0~360 → -180~180 변환
        # 예: 270° → -90°, 0° → 0°, 180° → -180° or 180°
        ds = ds.assign_coords(
            **{lon_coord: ((ds[lon_coord] + 180) % 360 - 180)}
        ).sortby(lon_coord)  # 변환 후 경도 오름차순 정렬

        # 변환된 파일을 원본 경로에 덮어씀
        # NetCDF는 직접 덮어쓰기가 안 되므로 임시 경로에 저장 후 교체
        tmp_path = nc_path.with_suffix(".tmp.nc")
        ds.to_netcdf(str(tmp_path))
        ds.close()

        # 원본 파일 교체 (임시 파일 → 원본 경로)
        tmp_path.replace(nc_path)

        logger.debug(f"  경도 변환 완료: {nc_path.name} (0~360 → -180~180)")

    def download_day(self, date: datetime, data_type: str, resolution: float) -> Path | None:
        """
        특정 날짜의 wind 또는 wave 데이터 1일치 다운로드

        Parameters
        ----------
        date       : datetime  →  다운로드할 날짜
        data_type  : str       →  "wind" 또는 "wave"
        resolution : float     →  공간 해상도

        Returns
        -------
        Path | None  →  성공 시 저장 경로, 실패 시 None
        """
        if data_type not in VARIABLES_MAP:
            raise ValueError(f"data_type은 'wind' 또는 'wave' 이어야 합니다. 입력값: {data_type}")

        output_path = self._get_output_path(date, data_type)

        # ── 이미 파일이 있으면 건너뜀 ──
        if output_path.exists() and output_path.stat().st_size > 0:
            size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"[건너뜀] {output_path.name} ({size_mb:.1f} MB)")
            return output_path

        logger.info(
            f"[다운로드] {date.strftime('%Y-%m-%d')} {data_type} "
            f"({len(VARIABLES_MAP[data_type])}변수, 해상도 {resolution}°)"
        )

        try:
            # CDS API 요청 전송 및 파일 다운로드
            request = self._build_request(date, data_type, resolution)
            result  = self.client.retrieve(name=self.DATASET, request=request)
            result.download(str(output_path))

            # ── 경도 변환: 0~360 → -180~180 ──
            # CDS API 반환 파일은 경도가 0~359.75로 저장됨
            # HYCOM은 -180~180을 사용하므로 ECMWF도 동일하게 맞춤
            self._fix_longitude(output_path)

            size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.success(f"[완료] {output_path.name} ({size_mb:.1f} MB)")
            return output_path

        except Exception as e:
            logger.error(f"[실패] {date.strftime('%Y-%m-%d')} {data_type}: {e}")
            # 실패로 생긴 불완전한 파일 삭제 (다음 실행 시 재시도 가능하도록)
            if output_path.exists():
                output_path.unlink()
            return None

    def run(self, json_path: Path, resolution: float) -> list[Path]:
        """
        down.json을 읽어 전체 날짜 범위의 wind + wave 다운로드 실행

        Parameters
        ----------
        json_path  : Path   →  config/down.json 경로
        resolution : float  →  공간 해상도

        Returns
        -------
        list[Path]  →  성공적으로 다운로드된 파일 경로 목록
        """
        # JSON에서 시작일/종료일 로드
        start_date, end_date = load_date_range(json_path)

        total_days    = (end_date - start_date).days + 1
        downloaded    = []   # 성공한 파일 경로 누적
        current_date  = start_date
        day_index     = 0

        logger.info(f"총 {total_days}일 × 2종류(wind, wave) = {total_days * 2}개 파일 예정")

        # 날짜 반복: 시작일부터 종료일까지 하루씩
        while current_date <= end_date:
            day_index += 1
            logger.info(f"[{day_index}/{total_days}일] {current_date.strftime('%Y-%m-%d')}")

            # wind와 wave 순서대로 다운로드
            for data_type in ("wind", "wave"):
                path = self.download_day(current_date, data_type, resolution)
                if path is not None:
                    downloaded.append(path)

            # 다음 날짜로 이동
            current_date += timedelta(days=1)

        # 최종 결과 요약
        expected = total_days * 2  # wind + wave 각 1파일씩
        logger.info(
            f"다운로드 완료 | 성공: {len(downloaded)}/{expected}개 파일"
        )
        return downloaded
