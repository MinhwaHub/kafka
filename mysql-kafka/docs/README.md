# MySQL → Kafka → Iceberg Data Pipeline

실시간 MySQL 데이터를 Kafka를 통해 S3 Iceberg로 전송하는 고성능 데이터 파이프라인입니다.

## 🏗️ 아키텍처 개요

```
MySQL Database → Producer Thread → Kafka → Consumer Thread → S3 Iceberg
     ↓               ↓               ↓          ↓              ↓
                  실시간 폴링         Topic     배치 처리      Parquet 저장
```

## 📁 프로젝트 구조

```
mysql-kafka/
├── main.py                    # 메인 실행 파일
├── pipeline_manager.py        # 파이프라인 관리자 (CLI 인터페이스)
├── pipeline.py                # 핵심 파이프라인 로직
├── config.py                  # 설정 파일
├── circuit_breaker.py         # Circuit Breaker 패턴 구현
├── slack_util.py              # Slack 알림 기능
├── requirements.txt           # Python 의존성
├── pipeline_status.json       # 파이프라인 상태 저장
├── pipeline_manager.log       # 관리자 로그
├── pipeline.out               # 파이프라인 stdout 로그
├── pipeline.err               # 파이프라인 stderr 로그
├── debug.log                  # 디버그 로그
└── docker-compose.yml         # Kafka 개발 환경
```

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# 의존성 설치
pip install -r requirements.txt

# Kafka 개발 환경 시작 (선택사항)
docker-compose up -d
```

### 2. 설정 확인

`config.py`에서 다음 설정을 확인하세요:

```python
# MySQL 연결 정보
MYSQL_CONFIG = {
    "host": "",
    "user": "", 
    "password": "",
    "database": "",
}

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9095"]

# S3 설정
S3_BUCKET_NAME = "data-pipeline-test"
AWS_SESSION_PROFILE = "dev-mina"
```

### 3. 파이프라인 실행

```bash
# 파이프라인 시작
python pipeline_manager.py start

# 상태 확인
python pipeline_manager.py status

# 파이프라인 중지
python pipeline_manager.py stop

# 재시작
python pipeline_manager.py restart
```

## 📊 현재 구성된 파이프라인

### 1. ERC721 Holders Pipeline
- **테이블**: `tx_collector.erc721_holders`
- **Kafka Topic**: `tx_collector_erc721_holders_topic`
- **대상 컬럼**: `_id` (증분 컬럼)
- **파티션**: `last_updated_time`
- **Iceberg 테이블**: `s3://data-pipeline-test/iceberg-warehouse/erc721_holders/`

### 2. ERC20 Holders Pipeline  
- **테이블**: `tx_collector.erc20_holders`
- **Kafka Topic**: `tx_collector_erc20_holders_topic`
- **대상 컬럼**: `_id` (증분 컬럼)
- **파티션**: `last_updated_time`
- **Iceberg 테이블**: `s3://data-pipeline-test/iceberg-warehouse/erc20_holders/`

## 🏔️ Apache Iceberg 데이터 레이크

### Iceberg 테이블 구조
```
S3: data-pipeline-test/
├── iceberg-warehouse/
│   ├── erc721_holders/
│   │   ├── metadata/
│   │   │   ├── version-hint.text
│   │   │   ├── v1.metadata.json
│   │   │   └── snap-*.avro
│   │   └── data/
│   │       └── 00000-0-*.parquet
│   └── erc20_holders/
│       ├── metadata/
│       │   ├── version-hint.text
│       │   ├── v1.metadata.json
│       │   └── snap-*.avro
│       └── data/
│           └── 00000-0-*.parquet
```

### 데이터 압축 및 최적화
- **파일 형식**: Parquet (컬럼형 저장)
- **압축**: Snappy 압축으로 저장 공간 최적화
- **파티셔닝**: `last_updated_time` 기준 일별 파티션
- **인덱싱**: Bloom Filter를 통한 빠른 데이터 검색
- **데이터 레이크**: Iceberg를 활용한 ACID 보장, 스냅샷 격리, 메타데이터 관리

## 🛠️ 관리 명령어

### 기본 명령어
```bash
# 전체 파이프라인 관리
python pipeline_manager.py start     # 모든 파이프라인 시작
python pipeline_manager.py stop      # 모든 파이프라인 중지
python pipeline_manager.py restart   # 모든 파이프라인 재시작
python pipeline_manager.py status    # 상태 확인

# 개별 파이프라인 관리
python pipeline_manager.py restart-single <pipeline_name>
python pipeline_manager.py stop-single <pipeline_name>

# 로그 확인
python pipeline_manager.py logs              # 최근 50줄
python pipeline_manager.py logs --lines 100 # 최근 100줄
vim pipeline.out 
vim pipeline.err
vim debug.log
```

### 파이프라인 이름
- `tx_collector_erc721_holders_pipeline`
- `tx_collector_erc20_holders_pipeline`

## 🔧 주요 기능

### 1. 실시간 데이터 동기화
- **증분 처리**: `_id` 컬럼 기반으로 새로운 데이터만 처리
- **배치 처리**: 최대 100개 레코드씩 처리
- **폴링 간격**: 10초마다 새 데이터 확인

### 2. Circuit Breaker 패턴
- **장애 감지**: 연속 5회 실패 시 자동 차단
- **복구 시도**: 60초 후 자동 복구 시도
- **상태 모니터링**: CLOSED/OPEN/HALF_OPEN 상태 추적

### 3. 자동 재시작 및 복구
- **최대 재시작**: 파이프라인당 10회까지 자동 재시작
- **상태 보존**: `pipeline_status.json`에 처리 상태 저장
- **Permanently Failed**: 재시작 한계 도달 시 자동 중단

### 4. 모니터링 및 알림
- **실시간 상태**: 30초마다 파이프라인 상태 확인
- **Slack 알림**: 장애 발생 시 Slack 채널 알림
- **상세 로깅**: 다양한 레벨의 로그 기록

### 5. Iceberg 데이터 레이크 트랜잭션
- **ACID 보장**: 데이터 무결성과 일관성 보장
- **스냅샷 격리**: 동시 읽기/쓰기 작업 지원
- **메타데이터 관리**: 테이블 스키마 및 파티션 정보 자동 관리
- **압축 최적화**: 자동 파일 압축 및 병합

## 📈 성능 특성

### 처리량
- **MySQL 배치**: 100 레코드/배치
- **Kafka 배치**: 1000 레코드 또는 300초마다 S3 업로드
- **폴링 간격**: 10초
- **Iceberg 커밋**: 배치당 1회 트랜잭션 커밋

### 안정성
- **Circuit Breaker**: 시스템 과부하 방지
- **자동 재시작**: 일시적 장애 자동 복구
- **상태 보존**: 프로세스 재시작 시 이어서 처리
- **트랜잭션 롤백**: 실패 시 자동 롤백으로 데이터 일관성 유지

### 저장소 효율성
- **Parquet 압축**: 평균 70-80% 저장 공간 절약
- **파티셔닝**: 쿼리 성능 최적화
- **메타데이터 캐싱**: 빠른 스키마 조회

## 🔍 모니터링

### 상태 확인
```bash
python pipeline_manager.py status
```

출력 예시:
```
📊 PIPELINE STATUS REPORT
================================================================================

🔧 Pipeline: tx_collector_erc721_holders_pipeline
   Status: 🟢 RUNNING
   Producer: ✅
   Consumer: ✅
   Last Processed ID: 188004
   Restart Count: 0/10
   Circuit Breaker: 🟢 CLOSED
   Consecutive Failures: 0
   Last Update: 2025-01-01 15:30:45
```

### 로그 파일
- **pipeline.out**: 표준 출력 로그
- **pipeline.err**: 에러 로그
- **pipeline_manager.log**: 관리자 로그
- **debug.log**: 상세 디버그 로그

## ⚙️ 설정 옵션

### Circuit Breaker 설정
```python
"circuit_breaker_failure_threshold": 5,      # 연속 실패 임계값
"circuit_breaker_recovery_timeout": 60,      # 복구 대기 시간(초)
"circuit_breaker_half_open_max_calls": 3,    # Half-open 상태 최대 시도
```

### 성능 튜닝
```python
MYSQL_POLL_INTERVAL_SECONDS = 10    # MySQL 폴링 간격
MYSQL_BATCH_SIZE = 100              # MySQL 배치 크기
S3_BATCH_MAX_RECORDS = 1000         # S3 업로드 배치 크기
S3_BATCH_MAX_SECONDS = 300          # S3 업로드 시간 간격
```

### Iceberg 설정
```python
# 파티션 구성
ICEBERG_PARTITION_SPEC = "day(last_updated_time)"

# 파일 형식 및 압축
ICEBERG_FILE_FORMAT = "parquet"
ICEBERG_COMPRESSION = "snappy"

# 스냅샷 보존 정책
ICEBERG_SNAPSHOT_RETENTION_DAYS = 7
```

## 🚨 문제 해결

### 일반적인 문제

1. **파이프라인이 시작되지 않음**
   ```bash
   # MySQL/Kafka 연결 확인
   python pipeline_manager.py status
   ```

2. **Permanently Failed 상태**
   ```bash
   # 재시작하여 실패 카운트 리셋
   python pipeline_manager.py restart
   ```

3. **높은 지연(Lag) 발생**
   - `pipeline_manager.py status`에서 Processing Lag 확인
   - MySQL 쿼리 성능 최적화 검토

4. **Iceberg 트랜잭션 실패**
   ```bash
   # S3 권한 및 Iceberg 메타데이터 확인
   # AWS CLI로 S3 접근 권한 테스트
   aws s3 ls s3://data-pipeline-test/iceberg-warehouse/
   ```

### 로그 분석
```bash
# 최근 에러 확인
python pipeline_manager.py logs --lines 200

# 실시간 로그 모니터링
tail -f pipeline.out

# Iceberg 관련 에러 확인
grep -i "iceberg\|transaction\|commit" pipeline.err
```

## 🔐 보안 고려사항

- **비밀번호 관리**: 환경 변수 또는 AWS Secrets Manager 사용 권장
- **네트워크 보안**: VPC 내부 통신 권장
- **IAM 권한**: S3 및 Iceberg 접근 권한 최소화
- **데이터 암호화**: S3 서버 사이드 암호화 (SSE-S3) 활성화

## 📊 데이터 품질 보장

### 데이터 검증
- **스키마 검증**: Iceberg 스키마 진화를 통한 데이터 타입 일관성
- **중복 제거**: `_id` 기반 유니크 키 보장
- **무결성 검사**: 트랜잭션 커밋 전 데이터 유효성 검증

### 모니터링 메트릭
- **처리량**: 분당 처리된 레코드 수
- **지연시간**: MySQL에서 S3까지의 end-to-end 지연
- **에러율**: 실패한 트랜잭션 비율
- **저장소 사용량**: Iceberg 테이블 크기 및 증가율

## 📝 라이센스

이 프로젝트는 내부 사용을 위한 것입니다. 