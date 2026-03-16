# llm_for_ship — 프로젝트 협업 규칙

## 프로젝트 개요
한화오션 선박 특화 LLM 개발 — 환경 데이터 수집/적재 파이프라인 (담당: 이훈정 책임)

## 작업 범위 (내 담당)
- ECMWF ERA5 재분석 데이터 수집 → PostgreSQL(TimescaleDB) 적재
- ECMWF 예보 데이터 수집 → DB 적재
- HYCOM 해류 데이터 수집 → DB 적재 (추후)

## 협업 규칙
1. **Spec-First**: 코드 작성 전 반드시 specs/*.md 명세 먼저 작성 → 승인 후 구현
2. **주석 필수**: 모든 코드 줄에 한국어 주석 (초보자도 이해 가능한 수준)
3. **커밋 금지**: 명시적으로 요청할 때만 커밋
4. **제안 후 승인**: 변경사항은 먼저 제안 → 승인 후 구현
5. **점진적 개발**: 테스트(1달치) → 검증 → 전체 적재 순서

## 개발 환경
- OS: Windows 11 + WSL (Ubuntu)
- Python: 3.12
- 패키지 관리: UV
- DB: PostgreSQL 16 + TimescaleDB (Docker)
- IDE: VS Code

## 기억할 것 (추후 구현)
- [ ] 매일 오전 8시 자동 실행 스케줄러 (cron 또는 APScheduler)
- [ ] HYCOM 해류 데이터 파이프라인
- [ ] ECMWF 예보 데이터 파이프라인
- [ ] 전체 과거 데이터 적재 (2021-01-01~현재)

## 데이터 흐름
```
CDS API (ECMWF ERA5)
    ↓ cdsapi (Python)
NetCDF 파일 (data/ecmwf/reanalysis/YYYY/MM/YYYYMMDD.nc)
    ↓ xarray + pandas
PostgreSQL + TimescaleDB (env_ecmwf_reanalysis 테이블)
    ↓
LLM Agent (질의응답, 보고서 생성)
```
