"""
Configuration settings for MySQL to Kafka to Iceberg Pipeline
"""

import os
import json
from typing import Dict, List, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# 🔧 MySQL → Kafka → S3 파이프라인 설정
# =============================================================================

# MySQL 설정 - 환경변수에서 로드
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}

# Kafka 설정 - 환경변수에서 로드
KAFKA_BOOTSTRAP_SERVERS = [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9095")]

# S3 설정 - 환경변수에서 로드
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "emr-data-pipeline-test")
AWS_SESSION_PROFILE = os.getenv("AWS_SESSION_PROFILE", "dev-mina")

# Slack 설정 - 환경변수에서 로드
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

# 파이프라인 설정 - 환경변수에서 로드
MYSQL_POLL_INTERVAL_SECONDS = int(os.getenv("MYSQL_POLL_INTERVAL_SECONDS", "10"))
MYSQL_BATCH_SIZE = int(os.getenv("MYSQL_BATCH_SIZE", "100"))
S3_BATCH_MAX_RECORDS = int(os.getenv("S3_BATCH_MAX_RECORDS", "1000"))
S3_BATCH_MAX_SECONDS = int(os.getenv("S3_BATCH_MAX_SECONDS", "300"))

# =============================================================================
# 📁 파일 경로 설정
# =============================================================================

# 기본 디렉토리 경로
BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)  # mysql-kafka 루트
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# 파이프라인 상태 파일
PIPELINE_STATUS_FILE = os.path.join(DATA_DIR, "pipeline_status.json")

# 로그 파일들
PIPELINE_STDOUT_LOG = os.path.join(LOGS_DIR, "pipeline.out")
PIPELINE_STDERR_LOG = os.path.join(LOGS_DIR, "pipeline.err")
PIPELINE_MANAGER_LOG = os.path.join(LOGS_DIR, "pipeline_manager.log")

# PID 및 명령 파일
MAIN_PIPELINE_PID_FILE = os.path.join(BASE_DIR, "main_pipeline.pid")
PIPELINE_COMMAND_FILE = os.path.join(BASE_DIR, "pipeline_command.txt")

# 메인 스크립트 경로
MAIN_SCRIPT_PATH = os.path.join(BASE_DIR, "src", "main.py")

# JAR 파일 경로 (Spark용)
JARS_DIR = os.getenv("SPARK_JARS_DIR", "/Users/mina/nx-mina/test")
SPARK_JARS = [
    os.path.join(JARS_DIR, "hadoop-aws-3.3.1.jar"),
    os.path.join(JARS_DIR, "aws-java-sdk-bundle-1.12.781.jar"),
    os.path.join(JARS_DIR, "iceberg-spark-runtime-3.5_2.12-1.9.0.jar"),
    os.path.join(JARS_DIR, "spark-avro_2.12-3.5.5.jar"),
]

# 필요한 디렉토리 생성
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# 🎯 파이프라인 설정 (여러 개 추가 가능!)
PIPELINES = [
    {
        "name": "tx_collector_zd_erc721_holders_pipeline",
        "enabled": True,
        "mysql": {
            "connection": MYSQL_CONFIG,
            "database": "tx_collector_zd",
            "table": "erc721_holders",
            "target_column": "_id",
            "select_columns": "_id, chain_id, hex(token) as token, token_id, hex(owner) as owner, last_updated_block, last_updated_time",
            "partition_column": "last_updated_time",
            "primary_key": "_id",
            # last_processed_id는 pipeline_status.json에서 자동 관리됩니다
            # 초기 실행 시에만 0부터 시작하고, 이후에는 저장된 값을 사용합니다
        },
        "kafka": {
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "topic": "tx_collector_zd_erc721_holders_topic",
        },
        "iceberg": {
            "warehouse_path": f"s3://{S3_BUCKET_NAME}/iceberg-warehouse/",
        },
    },
    {
        "name": "tx_collector_zd_erc20_holders_pipeline",
        "enabled": True,
        "mysql": {
            "connection": MYSQL_CONFIG,
            "database": "tx_collector_zd",
            "table": "erc20_holders",
            "target_column": "_id",
            "select_columns": "_id, chain_id, hex(token) as token, hex(account) as account, balance, last_updated_block, last_updated_time",
            "partition_column": "last_updated_time",
            "primary_key": "_id",
            # last_processed_id는 pipeline_status.json에서 자동 관리됩니다
            # 초기 실행 시에만 0부터 시작하고, 이후에는 저장된 값을 사용합니다
        },
        "kafka": {
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "topic": "tx_collector_zd_erc20_holders_topic",
        },
        "iceberg": {
            "warehouse_path": f"s3://{S3_BUCKET_NAME}/iceberg-warehouse/",
        },
    },
]

# Default pipeline configuration
DEFAULT_PIPELINE_CONFIG = {
    "circuit_breaker_failure_threshold": 5,
    "circuit_breaker_recovery_timeout": 60,
    "circuit_breaker_half_open_max_calls": 3,
    "stop_producer_on_iceberg_failure": False,
    "max_kafka_lag_tolerance": 10000,
    "max_restart_attempts": 10,
    "enabled": True,
}


def load_pipeline_config(
    config_file_path: str = "pipelines_config.json",
) -> List[Dict[str, Any]]:
    """Load pipeline configuration from JSON file or fallback to PIPELINES"""
    try:
        # Apply default settings to PIPELINES directly
        pipelines_with_defaults = []
        for pipeline in PIPELINES:
            if not pipeline.get("enabled", True):
                continue

            # Add default settings to existing pipeline config
            pipeline_with_defaults = pipeline.copy()
            for key, default_value in DEFAULT_PIPELINE_CONFIG.items():
                if key not in pipeline_with_defaults:
                    pipeline_with_defaults[key] = default_value

            pipelines_with_defaults.append(pipeline_with_defaults)

        print(
            f"✅ Using {len(pipelines_with_defaults)} pipelines from PIPELINES variable"
        )
        return pipelines_with_defaults

    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in configuration file: {e}")
        return []
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return []


# Example configuration structure (for reference)
EXAMPLE_PIPELINE_CONFIG = {
    "pipelines": [
        {
            "name": "example_pipeline",
            "enabled": True,
            "mysql": {
                "connection": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "username",
                    "password": "password",
                    "database": "database_name",
                },
                "database": "database_name",
                "table": "table_name",
                "target_column": "id",
                "partition_column": "created_at",
                "primary_key": "id",
                # last_processed_id는 pipeline_status.json에서 자동 관리됩니다
                # 초기 실행 시에만 0부터 시작하고, 이후에는 저장된 값을 사용합니다
            },
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "topic": "example_topic",
            },
            "iceberg": {"warehouse_path": "s3://your-bucket/warehouse/"},
            "slack": {
                "webhook_url": "https://hooks.slack.com/...",
                "channel": "#alerts",
            },
            "circuit_breaker_failure_threshold": 5,
            "circuit_breaker_recovery_timeout": 60,
            "circuit_breaker_half_open_max_calls": 3,
            "stop_producer_on_iceberg_failure": False,
            "max_kafka_lag_tolerance": 10000,
        }
    ]
}
