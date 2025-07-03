# MySQL → Kafka → Iceberg Data Pipeline Architecture

## 🏗️ 전체 시스템 아키텍처

```mermaid
graph TB
    subgraph "MySQL Database"
        DB1[(tx_collector<br/>erc721_holders)]
        DB2[(tx_collector<br/>erc20_holders)]
    end
    
    subgraph "Pipeline Manager"
        PM[Pipeline Manager<br/>pipeline_manager.py]
        MAIN[Main Process<br/>main.py]
        CB[Circuit Breaker<br/>circuit_breaker.py]
    end
    
    subgraph "Pipeline 1: ERC721"
        P1[Producer Thread 1]
        C1[Consumer Thread 1]
        K1[Kafka Topic<br/>erc721_holders_topic]
    end
    
    subgraph "Pipeline 2: ERC20"
        P2[Producer Thread 2]
        C2[Consumer Thread 2]
        K2[Kafka Topic<br/>erc20_holders_topic]
    end
    
    subgraph "S3 Iceberg"
        S3[(S3 Bucket<br/>emr-data-pipeline-test)]
        ICE1[Iceberg Table<br/>erc721_holders]
        ICE2[Iceberg Table<br/>erc20_holders]
    end
    
    subgraph "Monitoring & Control"
        STATUS[pipeline_status.json]
        LOGS[Log Files<br/>pipeline.out/err<br/>debug.log]
        SLACK[Slack Notifications]
    end
    
    DB1 --> P1
    DB2 --> P2
    P1 --> K1
    P2 --> K2
    K1 --> C1
    K2 --> C2
    C1 --> ICE1
    C2 --> ICE2
    ICE1 --> S3
    ICE2 --> S3
    
    PM --> P1
    PM --> P2
    PM --> C1
    PM --> C2
    PM --> STATUS
    PM --> LOGS
    
    CB --> P1
    CB --> P2
    CB --> C1
    CB --> C2
    
    MAIN --> PM
    PM --> SLACK
    
    style DB1 fill:#e3f2fd
    style DB2 fill:#e3f2fd
    style K1 fill:#fff8e1
    style K2 fill:#fff8e1
    style S3 fill:#e8f5e8
    style PM fill:#fce4ec
    style CB fill:#ffebee
```

## 🔄 파이프라인 상세 플로우

```mermaid
flowchart TD
    subgraph "Initialization"
        START[Start Pipeline Manager] --> LOAD_CONFIG[Load Configuration<br/>config.py]
        LOAD_CONFIG --> INIT_MANAGER[Initialize Pipeline Manager]
        INIT_MANAGER --> CREATE_PIPELINES[Create Pipeline Instances]
    end
    
    subgraph "Producer Flow (Per Pipeline)"
        PROD_START[Start Producer Thread] --> MYSQL_CONN[Connect to MySQL]
        MYSQL_CONN --> LOAD_STATUS[Load last_processed_id<br/>from pipeline_status.json]
        LOAD_STATUS --> POLL_LOOP{Poll Loop<br/>Every 10s}
        
        POLL_LOOP --> QUERY[SELECT * FROM table<br/>WHERE _id > last_processed_id<br/>LIMIT 100]
        QUERY --> CHECK_DATA{New Data?}
        
        CHECK_DATA -->|Yes| CB_CHECK{Circuit Breaker<br/>Can Execute?}
        CHECK_DATA -->|No| WAIT[Wait 10 seconds]
        WAIT --> POLL_LOOP
        
        CB_CHECK -->|Yes| SEND_KAFKA[Send to Kafka Topic]
        CB_CHECK -->|No| CB_WAIT[Circuit Breaker Wait]
        CB_WAIT --> POLL_LOOP
        
        SEND_KAFKA --> UPDATE_ID[Update last_processed_id]
        UPDATE_ID --> SAVE_STATUS[Save to pipeline_status.json]
        SAVE_STATUS --> POLL_LOOP
        
        SEND_KAFKA -->|Failure| RECORD_FAILURE[Record Circuit Breaker Failure]
        RECORD_FAILURE --> POLL_LOOP
    end
    
    subgraph "Consumer Flow (Per Pipeline)"
        CONS_START[Start Consumer Thread] --> KAFKA_CONN[Connect to Kafka]
        KAFKA_CONN --> INIT_BATCH[Initialize Batch<br/>message_batch = []]
        INIT_BATCH --> CONS_LOOP{Consumer Loop}
        
        CONS_LOOP --> POLL_KAFKA[Poll Kafka Messages<br/>timeout: 1s, max: 100]
        POLL_KAFKA --> CHECK_MSGS{Messages Available?}
        
        CHECK_MSGS -->|Yes| ADD_BATCH[Add to message_batch]
        CHECK_MSGS -->|No| CHECK_TIMEOUT{Batch Timeout?<br/>300 seconds}
        
        ADD_BATCH --> CHECK_BATCH_SIZE{Batch Size >= 1000<br/>or Timeout?}
        CHECK_BATCH_SIZE -->|Yes| PROCESS_BATCH[Process Batch]
        CHECK_BATCH_SIZE -->|No| CONS_LOOP
        
        CHECK_TIMEOUT -->|Yes & Not Empty| PROCESS_BATCH
        CHECK_TIMEOUT -->|No| CONS_LOOP
        
        PROCESS_BATCH --> CONVERT_DF[Convert to DataFrame]
        CONVERT_DF --> CONVERT_PARQUET[Convert to Parquet]
        CONVERT_PARQUET --> UPLOAD_S3[Upload to S3 Iceberg]
        UPLOAD_S3 --> RESET_BATCH[Reset Batch & Timer]
        RESET_BATCH --> CONS_LOOP
        
        UPLOAD_S3 -->|Success| CB_SUCCESS[Record Circuit Breaker Success]
        UPLOAD_S3 -->|Failure| CB_FAIL[Record Circuit Breaker Failure]
        CB_SUCCESS --> RESET_BATCH
        CB_FAIL --> RESET_BATCH
    end
    
    subgraph "Monitoring & Management"
        MONITOR[Monitor Thread<br/>Every 30s] --> CHECK_STATUS[Check Pipeline Status]
        CHECK_STATUS --> RESTART_CHECK{Need Restart?}
        RESTART_CHECK -->|Yes| AUTO_RESTART[Attempt Auto Restart]
        RESTART_CHECK -->|No| MONITOR
        
        AUTO_RESTART --> RESTART_COUNT{Restart Count<br/>< Max Attempts?}
        RESTART_COUNT -->|Yes| RESTART_PIPELINE[Restart Pipeline]
        RESTART_COUNT -->|No| MARK_FAILED[Mark as Permanently Failed]
        
        MARK_FAILED --> ALL_FAILED{All Pipelines<br/>Failed?}
        ALL_FAILED -->|Yes| SHUTDOWN[Shutdown Process]
        ALL_FAILED -->|No| MONITOR
        
        RESTART_PIPELINE --> MONITOR
    end
    
    style START fill:#e8f5e8
    style PROD_START fill:#e3f2fd
    style CONS_START fill:#fff3e0
    style MONITOR fill:#fce4ec
    style SHUTDOWN fill:#ffebee
```

## 📊 데이터 플로우 시퀀스

```mermaid
sequenceDiagram
    participant M as MySQL Database
    participant P as Producer Thread
    participant K as Kafka Topic
    participant C as Consumer Thread
    participant S as S3 Iceberg
    participant CB as Circuit Breaker
    participant PM as Pipeline Manager
    
    Note over M,PM: Pipeline Initialization
    PM->>P: Start Producer Thread
    PM->>C: Start Consumer Thread
    P->>M: Connect to Database
    C->>K: Connect to Kafka
    
    Note over M,PM: Real-time Data Processing
    loop Every 10 seconds
        P->>CB: Check can_execute()
        CB-->>P: True/False
        alt Circuit Breaker Open
            P->>P: Wait for recovery
        else Circuit Breaker Closed/Half-Open
            P->>M: Query new records (WHERE _id > last_id)
            M-->>P: Return batch (≤100 records)
            alt Data available
                P->>K: Send records to topic
                P->>P: Update last_processed_id
                P->>CB: Record success
            else No new data
                P->>P: Wait 10 seconds
            end
        end
    end
    
    loop Continuous consumption
        C->>K: Poll messages (timeout: 1s, max: 100)
        K-->>C: Return messages
        C->>C: Add to batch
        alt Batch conditions met (1000 records OR 300 seconds)
            C->>C: Convert to DataFrame
            C->>C: Convert to Parquet
            C->>S: Upload to Iceberg table
            alt Upload success
                C->>CB: Record success
            else Upload failure
                C->>CB: Record failure
            end
            C->>C: Reset batch and timer
        end
    end
    
    Note over M,PM: Monitoring & Recovery
    loop Every 30 seconds
        PM->>P: Check thread status
        PM->>C: Check thread status
        alt Thread died or failed
            PM->>PM: Attempt restart
            alt Restart count < max_attempts
                PM->>P: Restart producer
                PM->>C: Restart consumer
            else Max restarts exceeded
                PM->>PM: Mark as permanently failed
                alt All pipelines failed
                    PM->>PM: Shutdown process
                end
            end
        end
    end
```

## 🔧 Circuit Breaker 상태 다이어그램

```mermaid
stateDiagram-v2
    [*] --> CLOSED
    
    CLOSED --> OPEN : consecutive_failures >= threshold (5)
    OPEN --> HALF_OPEN : recovery_timeout (60s) elapsed
    HALF_OPEN --> CLOSED : success recorded
    HALF_OPEN --> OPEN : max_calls (3) reached with failures
    
    note right of CLOSED
        Normal operation
        All requests allowed
    end note
    
    note right of OPEN
        Circuit breaker tripped
        All requests blocked
        Wait for recovery timeout
    end note
    
    note right of HALF_OPEN
        Testing recovery
        Limited requests allowed
        Monitor for success/failure
    end note
```

## 📁 파일 구조 및 역할

```mermaid
graph LR
    subgraph "Core Files"
        MAIN[main.py<br/>🎯 Entry Point]
        PM[pipeline_manager.py<br/>🛠️ Management CLI]
        PIPE[pipeline.py<br/>⚡ Core Logic]
        CONFIG[config.py<br/>⚙️ Configuration]
    end
    
    subgraph "Support Files"
        CB[circuit_breaker.py<br/>🔄 Fault Tolerance]
        SLACK[slack_util.py<br/>📢 Notifications]
        REQ[requirements.txt<br/>📦 Dependencies]
    end
    
    subgraph "Runtime Files"
        STATUS[pipeline_status.json<br/>💾 State Persistence]
        LOGS[*.log, *.out, *.err<br/>📝 Logging]
    end
    
    subgraph "Infrastructure"
        DOCKER[docker-compose.yml<br/>🐳 Kafka Environment]
    end
    
    MAIN --> PM
    PM --> PIPE
    PIPE --> CONFIG
    PIPE --> CB
    PIPE --> SLACK
    PM --> STATUS
    PIPE --> LOGS
    
    style MAIN fill:#e8f5e8
    style PM fill:#e3f2fd
    style PIPE fill:#fff3e0
    style CONFIG fill:#fce4ec
```

## 🚀 배포 및 운영 플로우

```mermaid
flowchart TD
    subgraph "Development"
        DEV_START[개발 시작] --> CONFIG_SETUP[설정 파일 구성]
        CONFIG_SETUP --> LOCAL_TEST[로컬 테스트]
        LOCAL_TEST --> DOCKER_KAFKA[Docker Kafka 실행]
    end
    
    subgraph "Deployment"
        DEPLOY_START[배포 시작] --> ENV_SETUP[환경 변수 설정]
        ENV_SETUP --> DEPS_INSTALL[의존성 설치]
        DEPS_INSTALL --> PIPELINE_START[파이프라인 시작]
    end
    
    subgraph "Operations"
        MONITORING[모니터링] --> STATUS_CHECK[상태 확인]
        STATUS_CHECK --> HEALTH_OK{정상 동작?}
        HEALTH_OK -->|Yes| MONITORING
        HEALTH_OK -->|No| TROUBLESHOOT[문제 해결]
        
        TROUBLESHOOT --> LOG_ANALYSIS[로그 분석]
        LOG_ANALYSIS --> RESTART[재시작]
        RESTART --> MONITORING
    end
    
    subgraph "Commands"
        CMD_START[python pipeline_manager.py start]
        CMD_STATUS[python pipeline_manager.py status]
        CMD_STOP[python pipeline_manager.py stop]
        CMD_RESTART[python pipeline_manager.py restart]
        CMD_LOGS[python pipeline_manager.py logs]
    end
    
    DOCKER_KAFKA --> PIPELINE_START
    PIPELINE_START --> MONITORING
    
    STATUS_CHECK --> CMD_STATUS
    RESTART --> CMD_RESTART
    LOG_ANALYSIS --> CMD_LOGS
    
    style DEV_START fill:#e8f5e8
    style DEPLOY_START fill:#e3f2fd
    style MONITORING fill:#fff3e0
    style TROUBLESHOOT fill:#ffebee
```

## 📊 성능 메트릭스

### 처리 성능
- **MySQL 폴링**: 10초 간격
- **배치 크기**: 100 레코드/배치 (MySQL → Kafka)
- **Kafka 배치**: 1000 레코드 또는 300초 (Kafka → S3)
- **동시 파이프라인**: 2개 (ERC721, ERC20)

### 안정성 메트릭스
- **Circuit Breaker 임계값**: 연속 5회 실패
- **복구 시간**: 60초
- **최대 재시작**: 10회/파이프라인
- **모니터링 간격**: 30초

### 저장소 구조
```
S3: emr-data-pipeline-test/
├── iceberg-warehouse/
│   ├── erc721_holders/
│   │   ├── metadata/
│   │   └── data/
│   └── erc20_holders/
│       ├── metadata/
│       └── data/
``` 