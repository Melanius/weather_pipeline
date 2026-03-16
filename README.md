# llm_for_ship — 환경 데이터 파이프라인

## 최초 세팅 순서 (한 번만)

### 1. Docker Desktop 설치 (없으면)
https://www.docker.com/products/docker-desktop/

### 2. TimescaleDB 실행
```bash
cd /mnt/c/Users/hjlee/llm_for_ship
docker-compose up -d
```

### 3. UV 가상환경 생성 및 패키지 설치
```bash
# UV가 없으면 먼저 설치
curl -LsSf https://astral.sh/uv/install.sh | sh

# 가상환경 생성 + 패키지 설치
uv sync
```

### 4. .env 설정
```bash
cp .env.example .env
# .env 파일 안에 API 키, DB 정보 입력
```

### 5. DB 스키마 초기화
```bash
uv run python run.py --init-db
```

---

## 데이터 수집 실행

### 테스트 (최근 30일, 빠름)
```bash
uv run python run.py --mode test
```

### 전체 (2021-01-01~현재, 오래 걸림)
```bash
uv run python run.py --mode full
```

### 다운로드만 (DB 적재 나중에)
```bash
uv run python run.py --mode download_only
```

### DB 적재만 (파일은 이미 있을 때)
```bash
uv run python run.py --mode load_only
```

---

## 프로젝트 구조
```
llm_for_ship/
├── run.py                     ← 실행 진입점
├── docker-compose.yml         ← TimescaleDB Docker 설정
├── pyproject.toml             ← 패키지 의존성
├── .env                       ← API 키, DB 정보 (Git 제외)
├── config/settings.toml       ← 수집 변수, 해상도 등 설정
├── specs/                     ← 데이터 명세서
├── data/ecmwf/reanalysis/     ← 다운로드된 NetCDF 파일
├── logs/                      ← 실행 로그
└── src/env_pipeline/
    ├── ecmwf/era5_downloader.py  ← CDS API 다운로드
    ├── db/connection.py           ← DB 연결
    ├── db/schema.py               ← 테이블 생성
    ├── db/loader.py               ← NetCDF → DB 적재
    └── pipeline.py                ← 전체 흐름 조율
```
