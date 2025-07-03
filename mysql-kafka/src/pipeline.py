"""
MySQL to Kafka to Iceberg Pipeline Implementation
"""

import time
import threading
import json
import pandas as pd
import mysql.connector
from decimal import Decimal
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, from_unixtime, desc, row_number
from pyspark.sql.window import Window
from datetime import datetime
import sys
import os

# src 디렉토리에서 실행하는 경우 상위 디렉토리를 Python path에 추가
if os.path.basename(os.getcwd()) == "src":
    parent_dir = os.path.dirname(os.getcwd())
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

from utils.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerState,
    CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS,
)
from utils.slack_util import SlackMessenger

# config 파일에서 설정 가져오기 - 실행 위치에 따라 적응적 import
try:
    # src 디렉토리에서 실행하는 경우
    from config import (
        MYSQL_POLL_INTERVAL_SECONDS,
        S3_BATCH_MAX_RECORDS,
        S3_BATCH_MAX_SECONDS,
        AWS_SESSION_PROFILE,
        MYSQL_BATCH_SIZE,
        PIPELINE_STATUS_FILE,
        SPARK_JARS,
    )
except ImportError:
    try:
        # mysql-kafka 루트에서 실행하는 경우
        from src.config import (
            MYSQL_POLL_INTERVAL_SECONDS,
            S3_BATCH_MAX_RECORDS,
            S3_BATCH_MAX_SECONDS,
            AWS_SESSION_PROFILE,
            MYSQL_BATCH_SIZE,
            PIPELINE_STATUS_FILE,
            SPARK_JARS,
        )
    except ImportError:
        print("❌ Error: config.py file not found.")
        sys.exit(1)

# Global stop event (will be imported from main)
stop_event = None


def decimal_serializer(obj):
    """Custom JSON serializer for Decimal and datetime objects"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class MySQLKafkaS3Pipeline:
    """MySQL → Kafka → Iceberg 파이프라인 클래스"""

    def __init__(self, pipeline_config: dict, global_stop_event: threading.Event):
        """파이프라인 설정으로 초기화"""
        global stop_event
        stop_event = global_stop_event

        self.name = pipeline_config["name"]
        self.mysql_config = pipeline_config["mysql"]
        self.kafka_config = pipeline_config["kafka"]
        self.iceberg_config = pipeline_config["iceberg"]
        self.slack_config = pipeline_config.get("slack", {})

        # Database and table information
        self.database_name = self.mysql_config["database"]
        self.table_name = self.mysql_config["table"]
        self.target_column = self.mysql_config["target_column"]
        self.partition_column = self.mysql_config["partition_column"]
        self.primary_key = self.mysql_config["primary_key"]
        self.select_columns = self.mysql_config["select_columns"]

        # Kafka topic
        self.kafka_topic = self.kafka_config["topic"]

        # Initialize last processed ID - 우선순위: pipeline_status.json > config
        self.last_processed_id = self._load_last_processed_id()

        # Thread control
        self.stop_event = threading.Event()

        # Error tracking for limited alerts
        self.error_counts = {}
        self.last_error_time = {}
        self.error_limit = 5
        self.error_window_minutes = 60

        # Kafka Producer 재사용을 위한 인스턴스 변수 추가
        self.kafka_producer = None
        self.producer_last_used = 0
        self.producer_timeout = 300  # 5분간 미사용시 재생성

        # Iceberg catalog initialization
        self.iceberg_catalog = None
        self._init_iceberg_catalog()

        # Slack messenger
        slack_webhook_url = self.slack_config.get("webhook_url", "")
        self.slack_messenger = SlackMessenger(webhook_url=slack_webhook_url)

        # Circuit breaker for Iceberg uploads
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=pipeline_config.get(
                "circuit_breaker_failure_threshold", 5
            ),
            recovery_timeout=pipeline_config.get(
                "circuit_breaker_recovery_timeout", 60
            ),
            half_open_max_calls=pipeline_config.get(
                "circuit_breaker_half_open_max_calls", 3
            ),
        )

        # Configuration options
        self.stop_producer_on_iceberg_failure = pipeline_config.get(
            "stop_producer_on_iceberg_failure", False
        )
        self.max_kafka_lag_tolerance = pipeline_config.get(
            "max_kafka_lag_tolerance", 10000
        )

        print(
            f"[{self.name}] Pipeline initialized with circuit breaker (threshold: {self.circuit_breaker.failure_threshold})",
            flush=True,
        )
        print(
            f"[{self.name}] Stop producer on Iceberg failure: {self.stop_producer_on_iceberg_failure}",
            flush=True,
        )

        # Legacy compatibility - keeping enabled for pipeline manager
        self.enabled = pipeline_config.get("enabled", True)

        print(f"🚀 Pipeline '{self.name}' initialized:", flush=True)
        print(f"  - Table: {self.table_name}", flush=True)
        print(f"  - Kafka Topic: {self.kafka_topic}", flush=True)
        print(f"  - Target Column: {self.target_column}", flush=True)
        print(f"  - Partition Column: {self.partition_column}", flush=True)
        print(f"  - Last Processed ID: {self.last_processed_id}", flush=True)

    def _load_last_processed_id(self) -> int:
        """
        last_processed_id를 로드하는 통일된 방법
        우선순위: pipeline_status.json > config.py
        """
        try:
            # 1. pipeline_status.json에서 먼저 시도
            import os
            import json

            status_file = PIPELINE_STATUS_FILE
            if os.path.exists(status_file):
                with open(status_file, "r") as f:
                    status_data = json.load(f)

                if self.name in status_data:
                    saved_id = status_data[self.name].get("last_processed_id", 0)
                    if saved_id > 0:
                        print(
                            f"[{self.name}] 📋 Loaded last_processed_id from status file: {saved_id} (resume from last position)",
                            flush=True,
                        )
                        return saved_id
                    else:
                        print(
                            f"[{self.name}] 📋 Status file exists but last_processed_id is 0 (starting fresh)",
                            flush=True,
                        )
                        return 0
                else:
                    print(
                        f"[{self.name}] 📋 Pipeline not found in status file (first time run)",
                        flush=True,
                    )
            else:
                print(
                    f"[{self.name}] 📋 No status file found (first time run)",
                    flush=True,
                )

            # 2. status 파일에 값이 없으면 config에서 가져오기 (fallback)
            config_id = self.mysql_config.get("last_processed_id", 0)
            if config_id > 0:
                print(
                    f"[{self.name}] 📋 Using last_processed_id from config: {config_id}",
                    flush=True,
                )
            else:
                print(
                    f"[{self.name}] 📋 Starting from beginning (last_processed_id = 0)",
                    flush=True,
                )
            return config_id

        except Exception as e:
            print(
                f"[{self.name}] ⚠️ Failed to load last_processed_id: {e}. Starting from 0.",
                flush=True,
            )
            return 0

    def send_slack_alert(
        self, message: str, message_type: str = "info", detailed_error: str = None
    ) -> None:
        """Slack 알림 전송"""
        try:
            # 메시지 타입에 따라 색상과 아이콘 결정
            if message_type == "error":
                color = "#ff0000"  # 빨간색
                alert_message = f"🚨 *{self.name} Pipeline Error*"
            elif message_type == "warning":
                color = "#ffa500"  # 주황색
                alert_message = f"⚠️ *{self.name} Pipeline Warning*"
            else:  # info
                color = "#36a64f"  # 초록색
                alert_message = f"ℹ️ *{self.name} Pipeline Info*"

            # 상세 에러가 있으면 block_message로 분리
            block_message = None
            if detailed_error:
                # Slack 메시지 길이 제한 (최대 2500자)
                max_length = 2500
                truncated_error = detailed_error[:max_length]
                if len(detailed_error) > max_length:
                    truncated_error += "\n... (truncated)"

                block_message = [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"📋 *Details:*\n```{truncated_error}```",
                        },
                    }
                ]
                # 본문에는 간단한 요약만
                alert_message += f"\n{message}"
            else:
                # 상세 에러가 없으면 기존 방식
                alert_message += f"\n{message}"

            self.slack_messenger.send_slack(
                text=alert_message, block_message=block_message, color=color
            )
            print(
                f"[{self.name}] Slack alert sent ({message_type}): {message}",
                flush=True,
            )
        except Exception as e:
            print(f"[{self.name}] Failed to send Slack alert: {e}", flush=True)

    def send_limited_slack_alert(
        self,
        message: str,
        error_key: str,
        message_type: str = "error",
        detailed_error: str = None,
    ) -> None:
        """제한된 횟수로 Slack 알림 전송 (같은 에러 최대 5번까지만)"""
        try:
            current_time = time.time()

            # 시간 윈도우 체크 (60분)
            if error_key in self.last_error_time:
                time_diff = current_time - self.last_error_time[error_key]
                if (
                    time_diff > self.error_window_minutes * 60
                ):  # 60분 경과시 카운트 리셋
                    self.error_counts[error_key] = 0

            # 에러 카운트 확인
            current_count = self.error_counts.get(error_key, 0)

            if current_count < self.error_limit:
                # 슬랙 알림 전송
                self.send_slack_alert(message, message_type, detailed_error)

                # 카운트 증가
                self.error_counts[error_key] = current_count + 1
                self.last_error_time[error_key] = current_time

                print(
                    f"[{self.name}] Limited slack alert sent ({current_count + 1}/{self.error_limit}): {error_key}",
                    flush=True,
                )
            else:
                print(
                    f"[{self.name}] Slack alert limit reached for {error_key} (suppressed)",
                    flush=True,
                )

        except Exception as e:
            print(f"[{self.name}] Failed to send limited Slack alert: {e}", flush=True)

    def reset_error_count(self, error_key: str) -> None:
        """Reset error count for a specific error key"""
        if error_key in self.error_counts:
            del self.error_counts[error_key]
        if error_key in self.last_error_time:
            del self.last_error_time[error_key]

    def _init_iceberg_catalog(self) -> None:
        """Initialize Iceberg catalog with AWS credentials"""
        try:
            session = boto3.Session(profile_name=AWS_SESSION_PROFILE)
            credentials = session.get_credentials()

            if not credentials:
                error_msg = "AWS credentials not found"
                print(f"[{self.name}] {error_msg}", flush=True)
                self.send_limited_slack_alert(
                    error_msg, "aws_credentials_not_found", message_type="error"
                )
                return

            self.iceberg_catalog = {
                "credentials": credentials,
                "warehouse_path": self.iceberg_config["warehouse_path"],
            }
            print(f"[{self.name}] Iceberg catalog initialized successfully", flush=True)

        except Exception as e:
            error_msg = f"Failed to initialize Iceberg catalog: {e}"
            print(f"[{self.name}] {error_msg}", flush=True)
            self.send_limited_slack_alert(
                error_msg,
                "iceberg_catalog_init_failed",
                message_type="error",
                detailed_error=str(e),
            )

    def get_mysql_connection(self) -> mysql.connector.connection.MySQLConnection:
        """MySQL 연결 생성"""
        try:
            connection = mysql.connector.connect(**self.mysql_config["connection"])
            # print(f"[{self.name}] MySQL connection established", flush=True)
            return connection
        except Exception as e:
            error_msg = f"MySQL connection failed: {e}"
            print(f"[{self.name}] {error_msg}", flush=True)
            self.send_limited_slack_alert(
                error_msg,
                "mysql_connection_failed",
                message_type="error",
                detailed_error=str(e),
            )
            return None

    def get_max_id_from_mysql(self, conn) -> int:
        """MySQL에서 최대 ID 조회"""
        try:
            cursor = conn.cursor()
            query = f"SELECT MAX({self.target_column}) FROM {self.table_name}"
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()

            max_id = result[0] if result and result[0] is not None else 0
            print(f"[{self.name}] Current max ID in MySQL: {max_id}", flush=True)
            return max_id

        except Exception as e:
            error_msg = f"Failed to get max ID from MySQL: {e}"
            print(f"[{self.name}] {error_msg}", flush=True)
            self.send_limited_slack_alert(
                error_msg,
                "mysql_max_id_query_failed",
                message_type="error",
                detailed_error=str(e),
            )
            return 0

    def fetch_data_from_mysql(self, conn, last_id: int) -> list:
        """MySQL에서 새로운 데이터 가져오기"""
        try:
            cursor = conn.cursor(dictionary=True)
            query = f"SELECT {self.select_columns} FROM {self.table_name} WHERE {self.target_column} > %s ORDER BY {self.target_column} ASC LIMIT {MYSQL_BATCH_SIZE}"

            cursor.execute(query, (last_id,))
            results = cursor.fetchall()
            cursor.close()

            if results:
                print(
                    f"[{self.name}] Executing query: {query} with last_id={last_id}",
                    flush=True,
                )
                first_id = results[0][self.target_column]
                last_id_fetched = results[-1][self.target_column]
                print(
                    f"[{self.name}] Fetched {len(results)} new records from MySQL (ID range: {first_id} ~ {last_id_fetched})",
                    flush=True,
                )
                return results
            else:
                #     print(f"[{self.name}] No new data found after ID {last_id}", flush=True)
                return []

        except Exception as e:
            error_msg = f"Failed to fetch data from MySQL with query '{query}': {e}"
            print(f"[{self.name}] {error_msg}", flush=True)
            self.send_limited_slack_alert(
                error_msg,
                "mysql_fetch_failed",
                message_type="error",
                detailed_error=str(e),
            )
            return []

    def get_or_create_kafka_producer(self) -> KafkaProducer:
        """Kafka Producer 생성 및 재사용 (개선된 버전)"""
        current_time = time.time()

        # 기존 Producer가 있고 아직 유효한 경우 재사용
        if (
            self.kafka_producer
            and current_time - self.producer_last_used < self.producer_timeout
        ):
            try:
                # Producer 연결 상태 간단 체크 (메타데이터 요청)
                self.kafka_producer.bootstrap_connected()
                self.producer_last_used = current_time
                print(
                    f"[{self.name}] ✅ Reusing existing Kafka producer (age: {current_time - self.producer_last_used:.1f}s)",
                    flush=True,
                )
                return self.kafka_producer
            except Exception as e:
                print(
                    f"[{self.name}] Existing producer connection failed: {e}, creating new one",
                    flush=True,
                )
                # 기존 Producer 정리
                try:
                    self.kafka_producer.close(timeout=5)
                except:
                    pass
                self.kafka_producer = None
        elif self.kafka_producer:
            # Producer가 있지만 timeout된 경우
            age = current_time - self.producer_last_used
            print(
                f"[{self.name}] Producer timeout (age: {age:.1f}s > {self.producer_timeout}s), creating new one",
                flush=True,
            )
            try:
                self.kafka_producer.close(timeout=5)
            except:
                pass
            self.kafka_producer = None

        # 새로운 Producer 생성

        try:
            print(f"[{self.name}] 🔄 Creating new Kafka producer", flush=True)
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_config["bootstrap_servers"],
                value_serializer=lambda v: json.dumps(
                    v, default=decimal_serializer
                ).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                # 연결 관련 설정 추가
                reconnect_backoff_ms=1000,
                reconnect_backoff_max_ms=10000,
                request_timeout_ms=30000,
                # Producer 재사용을 위한 설정
                max_block_ms=10000,  # 빠른 실패를 위해
            )

            self.kafka_producer = producer
            self.producer_last_used = current_time
            print(
                f"[{self.name}] ✅ New Kafka producer created successfully", flush=True
            )
            return producer

        except Exception as e:
            error_msg = f"Failed to create Kafka producer: {e}"
            print(f"[{self.name}] {error_msg}", flush=True)
            self.send_limited_slack_alert(
                error_msg,
                "kafka_producer_creation_failed",
                message_type="error",
                detailed_error=str(e),
            )
            return None

    def send_to_kafka_with_retry(self, data: list, max_retries: int = 3) -> bool:
        """재시도 로직이 포함된 Kafka 전송"""
        for attempt in range(max_retries):
            producer = self.get_or_create_kafka_producer()
            if not producer:
                print(
                    f"[{self.name}] No Kafka producer available (attempt {attempt + 1})",
                    flush=True,
                )
                time.sleep(2**attempt)  # 지수 백오프
                continue

            try:
                for record in data:
                    key = str(record[self.target_column])
                    producer.send(self.kafka_topic, key=key, value=record)

                producer.flush()
                print(
                    f"[{self.name}] Sent {len(data)} records to Kafka topic: {self.kafka_topic}",
                    flush=True,
                )
                return True

            except Exception as e:
                error_msg = f"Failed to send data to Kafka (attempt {attempt + 1}): {e}"
                print(f"[{self.name}] {error_msg}", flush=True)

                # 연결 오류인 경우 Producer 재생성
                if "connection" in str(e).lower() or "timeout" in str(e).lower():
                    self.kafka_producer = None

                if attempt == max_retries - 1:  # 마지막 시도
                    self.send_limited_slack_alert(
                        f"Kafka send failed after {max_retries} attempts",
                        "kafka_send_failed",
                        message_type="error",
                        detailed_error=str(e),
                    )
                else:
                    time.sleep(2**attempt)  # 지수 백오프

        return False

    def create_kafka_consumer(self) -> KafkaConsumer:
        """Kafka Consumer 생성"""
        try:

            def safe_value_deserializer(m):
                try:
                    return json.loads(m.decode("utf-8"))
                except UnicodeDecodeError as e:
                    print(
                        f"[{self.name}] UTF-8 decode error in Kafka message: {e}",
                        flush=True,
                    )
                    # Try with error handling
                    return json.loads(m.decode("utf-8", errors="replace"))
                except json.JSONDecodeError as e:
                    print(
                        f"[{self.name}] JSON decode error in Kafka message: {e}",
                        flush=True,
                    )
                    return None

            def safe_key_deserializer(m):
                try:
                    return m.decode("utf-8") if m else None
                except UnicodeDecodeError as e:
                    print(
                        f"[{self.name}] UTF-8 decode error in Kafka key: {e}",
                        flush=True,
                    )
                    return m.decode("utf-8", errors="replace") if m else None

            consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_config["bootstrap_servers"],
                value_deserializer=safe_value_deserializer,
                key_deserializer=safe_key_deserializer,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=f"{self.name}_consumer_group",
                consumer_timeout_ms=1000,
            )
            print(f"[{self.name}] Kafka consumer created successfully", flush=True)
            return consumer
        except Exception as e:
            error_msg = f"Failed to create Kafka consumer: {e}"
            print(f"[{self.name}] {error_msg}", flush=True)
            self.send_limited_slack_alert(
                error_msg,
                "kafka_consumer_creation_failed",
                message_type="error",
                detailed_error=str(e),
            )
            return None

    def upload_to_iceberg(self, data: list) -> bool:
        """Upload data to Iceberg table using Spark SQL with real MERGE INTO operations."""
        if not data:
            print(f"[{self.name}] No data to upload to Iceberg.", flush=True)
            return True

        # Check circuit breaker before attempting upload
        if not self.circuit_breaker.can_execute():
            cb_status = self.circuit_breaker.get_status()
            print(
                f"[{self.name}] Circuit breaker {cb_status['state']} - skipping Iceberg upload",
                flush=True,
            )
            print(
                f"[{self.name}] Consecutive failures: {cb_status['consecutive_failures']}",
                flush=True,
            )
            if cb_status["state"] == CircuitBreakerState.OPEN:
                print(
                    f"[{self.name}] Time until recovery attempt: {cb_status['time_until_recovery']:.1f}s",
                    flush=True,
                )
            return False

        if not self.iceberg_catalog:
            error_msg = "Iceberg catalog not initialized"
            print(f"[{self.name}] {error_msg}", flush=True)
            self.send_limited_slack_alert(
                error_msg, "iceberg_catalog_not_initialized", message_type="error"
            )
            self.circuit_breaker.record_failure()
            return False

        print(f"[{self.name}] --- Spark SQL MERGE INTO Operation ---", flush=True)
        print(
            f"[{self.name}] Circuit breaker state: {self.circuit_breaker.state}",
            flush=True,
        )

        # Convert and validate data
        if not isinstance(data, list) or not all(
            isinstance(item, dict) for item in data
        ):
            if isinstance(data, dict):
                data = [data]
            else:
                error_msg = "Invalid data format for Iceberg upload"
                self.send_limited_slack_alert(
                    error_msg, "invalid_data_format", message_type="error"
                )
                self.circuit_breaker.record_failure()
                return False

        if not data:
            return True

        try:
            # Use Spark SQL for real Iceberg table operations
            from pyspark.sql import SparkSession
            from pyspark.sql.types import StructType, StructField, LongType, IntegerType

            print(
                "**** this is data that is going to be uploaded to iceberg", flush=True
            )
            print(data, flush=True)
            df = pd.DataFrame(data)
            new_ids = df[self.target_column]
            new_min = int(new_ids.min())
            new_max = int(new_ids.max())

            print(
                f"[{self.name}] New data range: {new_min} ~ {new_max} ({len(df)} records)",
                flush=True,
            )
            print("Update Iceberg Data Size: ", len(data), flush=True)

            # Setup Spark session with Iceberg and S3 configuration
            credentials = self.iceberg_catalog["credentials"]
            warehouse_path = self.iceberg_catalog["warehouse_path"]

            # Convert s3:// to s3a:// for Spark
            warehouse_s3a = warehouse_path.replace("s3://", "s3a://")
            jars_str = ",".join(SPARK_JARS)

            spark = (
                SparkSession.builder.appName(f"IcebergMerge_{self.name}")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.jars", jars_str)
                .config(
                    "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
                .config("spark.hadoop.fs.s3a.access.key", credentials.access_key)
                .config("spark.hadoop.fs.s3a.secret.key", credentials.secret_key)
                .config("spark.hadoop.fs.s3a.session.token", credentials.token)
                .config(
                    "spark.sql.catalog.s3cat", "org.apache.iceberg.spark.SparkCatalog"
                )
                .config("spark.sql.catalog.s3cat.type", "hadoop")
                .config(
                    "spark.sql.catalog.s3cat.warehouse",
                    warehouse_s3a,
                )
                .config(
                    "spark.sql.catalog.s3cat.io-impl",
                    "org.apache.iceberg.hadoop.HadoopFileIO",
                )
                .getOrCreate()
            )

            # Get partition info
            sample_unixtime = df[self.partition_column].iloc[0]
            from datetime import datetime

            dt = datetime.fromtimestamp(sample_unixtime)
            date_str = dt.strftime("%Y-%m-%d")

            print(f"[{self.name}] Target partition: dt={date_str}", flush=True)

            # Table name and namespace
            table_identifier = f"s3cat.{self.database_name}.{self.table_name}"

            # Convert pandas DataFrame to Spark DataFrame
            spark_df = spark.createDataFrame(df)

            # Add the timestamp column for partitioning
            spark_df = spark_df.withColumn(
                "dt_utc",
                date_format(from_unixtime(col(self.partition_column)), "yyyy-MM-dd"),
            )

            print(f"[{self.name}] Source DataFrame Schema (from Kafka):", flush=True)
            spark_df.printSchema()

            # Create namespace if not exists
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3cat.{self.database_name}")

            # Debug: Compare with target schema
            try:
                if spark.catalog.tableExists(table_identifier):
                    target_df = spark.table(table_identifier)
                    print(f"[{self.name}] Target Iceberg Table Schema:", flush=True)
                    target_df.printSchema()
                else:
                    print(
                        f"[{self.name}] Target table {table_identifier} does not exist yet. No schema to compare.",
                        flush=True,
                    )
            except Exception as schema_e:
                print(
                    f"[{self.name}] Could not retrieve target table schema for comparison: {schema_e}",
                    flush=True,
                )

            # Generate CREATE TABLE schema automatically from DataFrame
            def spark_type_to_sql_type(spark_type):
                """Convert Spark data types to SQL types"""
                type_str = str(spark_type).lower()
                if "bigint" in type_str or "long" in type_str:
                    return "BIGINT"
                elif "int" in type_str:
                    return "INT"
                elif "string" in type_str:
                    return "STRING"
                elif "double" in type_str or "float" in type_str:
                    return "DOUBLE"
                elif "timestamp" in type_str:
                    return "TIMESTAMP"
                elif "date" in type_str:
                    return "DATE"
                elif "boolean" in type_str:
                    return "BOOLEAN"
                else:
                    return "STRING"  # Default fallback

            # Get schema from complete Spark DataFrame (including dt_utc)
            schema_fields = []
            for field in spark_df.schema.fields:
                sql_type = spark_type_to_sql_type(field.dataType)
                schema_fields.append(f"    {field.name} {sql_type}")

            schema_definition = ",\n".join(schema_fields)

            # Create table if not exists with auto-generated schema
            create_table_sql = f"""
                        CREATE TABLE IF NOT EXISTS {table_identifier} (
            {schema_definition}
                        )
                        USING iceberg
                    """
            # print(create_table_sql, flush=True)

            spark.sql(create_table_sql)
            print(
                f"[{self.name}] Iceberg table ensured: {table_identifier}", flush=True
            )
            print(
                f"[{self.name}] Auto-generated schema:\n{schema_definition}", flush=True
            )

            # Deduplicate source data to prevent MERGE cardinality violation
            # Keep the latest record based on target_column if duplicates on primary key exist
            pk_cols = (
                self.primary_key
                if isinstance(self.primary_key, list)
                else [self.primary_key]
            )
            # Use partition_column to determine the latest record, as it indicates the update time
            window_spec = Window.partitionBy(*pk_cols).orderBy(
                desc(self.partition_column)
            )
            dedup_spark_df = (
                spark_df.withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num")
            )

            # Check if deduplication removed any records
            original_count = spark_df.count()
            dedup_count = dedup_spark_df.count()
            if original_count > dedup_count:
                print(
                    f"[{self.name}] ⚠️ Deduplicated source data: {original_count} -> {dedup_count} records.",
                    flush=True,
                )

            # Create temporary view for MERGE operation using the deduplicated data
            temp_view_name = f"new_data_{self.name}_{int(time.time())}"
            dedup_spark_df.createOrReplaceTempView(temp_view_name)

            # Generate ON condition for MERGE operation based on primary key
            # If one condition, use string, if multiple conditions, use list
            if isinstance(self.primary_key, list):
                on_conditions = " AND ".join(
                    [f"target.{key} = source.{key}" for key in self.primary_key]
                )
            else:
                on_conditions = f"target.{self.primary_key} = source.{self.primary_key}"

            # Execute MERGE INTO operation
            merge_sql = f"""
                MERGE INTO {table_identifier} AS target
                USING {temp_view_name} AS source
                ON {on_conditions}

                WHEN MATCHED THEN
                UPDATE SET *
                
                WHEN NOT MATCHED THEN
                INSERT *
            """

            print(f"[{self.name}] Executing Spark SQL MERGE INTO...", flush=True)
            print(merge_sql, flush=True)
            spark.sql(merge_sql)

            print(f"[{self.name}] MERGE INTO completed successfully!", flush=True)
            print(
                f"[{self.name}] Processed {len(df)} records with Spark SQL", flush=True
            )

            # Clean up
            spark.catalog.dropTempView(temp_view_name)

            # Record success in circuit breaker
            self.circuit_breaker.record_success()

            # Reset error count on success
            self.reset_error_count("spark_merge_failed")

            # 🆕 S3/Iceberg merge 성공 후 JSON 파일에만 last_processed_id 저장
            # 메모리의 last_processed_id는 Producer에서 이미 업데이트됨
            # 여기서는 영구 저장을 위한 JSON 파일 업데이트만 수행
            self._update_pipeline_status_file()

            print(f"[{self.name}] Iceberg upload completed successfully!", flush=True)
            return True

        except ImportError as e:
            error_msg = f"PySpark not available: {e}"
            print(f"[{self.name}] {error_msg}", flush=True)
            self.send_limited_slack_alert(
                "PySpark not available",
                "pyspark_not_available",
                message_type="error",
                detailed_error=str(e),
            )
            self.circuit_breaker.record_failure()
            return False
        except Exception as e:
            error_msg = f"Spark SQL MERGE failed: {e}"
            print(f"[{self.name}] {error_msg}", flush=True)

            # Enhanced error reporting with circuit breaker status
            cb_status = self.circuit_breaker.get_status()
            detailed_error = f"Error: {str(e)}\nCircuit breaker failures: {cb_status['consecutive_failures']}/{self.circuit_breaker.failure_threshold}"

            self.send_limited_slack_alert(
                "Spark SQL MERGE operation failed",
                "spark_merge_failed",
                message_type="error",
                detailed_error=detailed_error,
            )
            self.circuit_breaker.record_failure()
            return False

    def mysql_to_kafka_producer_job(self):
        """Periodically fetches new data from MySQL and sends it to Kafka."""
        print(f"[{self.name}] Starting MySQL to Kafka producer job...", flush=True)

        try:

            while not self.stop_event.is_set() and not stop_event.is_set():
                mysql_conn = self.get_mysql_connection()
                if not mysql_conn:
                    error_msg = f"MySQL to Kafka producer job failed to start - no MySQL connection"
                    print(
                        f"[{self.name}] MySQL connection failed in producer job. Exiting.",
                        flush=True,
                    )
                    self.send_limited_slack_alert(
                        error_msg,
                        "mysql_producer_connection_failed",
                        message_type="error",
                    )
                    return

                last_processed_id = self.last_processed_id
                print(
                    f"[{self.name}] Producer starting with saved last_processed_id = {last_processed_id}",
                    flush=True,
                )
                new_data = self.fetch_data_from_mysql(
                    mysql_conn, last_id=last_processed_id
                )
                if new_data:
                    # 개선된 재시도 로직 사용
                    if self.send_to_kafka_with_retry(new_data):
                        # 🆕 Kafka 전송 성공 시 메모리의 last_processed_id 즉시 업데이트
                        processed_ids = [
                            int(record[self.target_column]) for record in new_data
                        ]
                        self.last_processed_id = max(processed_ids)
                        print(
                            f"[{self.name}] Producer sent {len(new_data)} records to Kafka successfully, updated memory last_processed_id to {self.last_processed_id}",
                            flush=True,
                        )
                    else:
                        print(
                            f"[{self.name}] Failed to send data to Kafka after retries. Retrying next cycle.",
                            flush=True,
                        )

                # Wait for the poll interval or until stop_event is set
                if not self.stop_event.is_set() and not stop_event.is_set():
                    self.stop_event.wait(MYSQL_POLL_INTERVAL_SECONDS)
                mysql_conn.close()

        except Exception as e:
            error_msg = f"Critical error in MySQL to Kafka producer job: {e}"
            print(
                f"[{self.name}] Error in MySQL to Kafka producer job: {e}", flush=True
            )
            self.send_limited_slack_alert(
                error_msg, "mysql_producer_critical_error", message_type="error"
            )
        finally:
            print(f"[{self.name}] MySQL to Kafka producer job stopping...", flush=True)
            # Producer 정리
            if self.kafka_producer:
                try:
                    self.kafka_producer.close(timeout=10)
                    print(f"[{self.name}] Kafka producer closed properly", flush=True)
                except Exception as e:
                    print(
                        f"[{self.name}] Error closing Kafka producer: {e}", flush=True
                    )
                finally:
                    self.kafka_producer = None
            if "mysql_conn" in locals() and mysql_conn:
                mysql_conn.close()
                print(f"[{self.name}] MySQL connection closed in job.", flush=True)

    def kafka_to_iceberg_consumer_job(self):
        """Consumes data from Kafka and uploads it to Iceberg table with circuit breaker protection."""
        print(f"[{self.name}] Starting Kafka to Iceberg consumer job...", flush=True)

        try:
            kafka_consumer = self.create_kafka_consumer()
            if not kafka_consumer:
                error_msg = f"Kafka to Iceberg consumer job failed to start - Kafka consumer creation failed"
                print(
                    f"[{self.name}] Kafka consumer creation failed. Exiting consumer job.",
                    flush=True,
                )
                self.send_limited_slack_alert(
                    error_msg, "kafka_consumer_creation_failed", message_type="error"
                )
                return

            print(
                f"[{self.name}] ✅ Kafka consumer created successfully, starting to poll messages...",
                flush=True,
            )

            message_batch = []
            last_upload_time = time.time()
            circuit_breaker_pause_until = 0
            poll_count = 0

            while not self.stop_event.is_set() and not stop_event.is_set():
                poll_count += 1

                # 주기적으로 Consumer 상태 로그 출력 (10번 poll마다)
                # if poll_count % 10 == 0:
                #     print(
                #         f"[{self.name}] Consumer polling... (poll #{poll_count}, batch size: {len(message_batch)})",
                #         flush=True,
                #     )

                # Check if we should pause due to circuit breaker
                current_time = time.time()
                if (
                    self.circuit_breaker.state == CircuitBreakerState.OPEN
                    and current_time < circuit_breaker_pause_until
                ):
                    print(
                        f"[{self.name}] Circuit breaker OPEN - pausing consumer for {circuit_breaker_pause_until - current_time:.1f}s",
                        flush=True,
                    )
                    time.sleep(min(5, circuit_breaker_pause_until - current_time))
                    continue

                # Reset pause time when circuit breaker allows operations
                if self.circuit_breaker.state != CircuitBreakerState.OPEN:
                    circuit_breaker_pause_until = 0

                # Poll Kafka for messages
                message_records = kafka_consumer.poll(
                    timeout_ms=1000, max_records=S3_BATCH_MAX_RECORDS
                )

                if not message_records:
                    # 메시지가 없을 때도 로그 출력 (처음 몇 번만)
                    if poll_count <= 5:
                        print(
                            f"[{self.name}] No messages received from Kafka (poll #{poll_count})",
                            flush=True,
                        )

                    # Check if it's time to upload an incomplete batch due to timeout
                    if message_batch and (
                        time.time() - last_upload_time >= S3_BATCH_MAX_SECONDS
                    ):
                        print(
                            f"[{self.name}] Time limit reached for batch. Uploading {len(message_batch)} records.",
                            flush=True,
                        )
                        # 중복 제거: primary key 기준
                        try:
                            df = pd.DataFrame(message_batch)
                            deduped_batch = df.drop_duplicates(
                                subset=self.primary_key, keep="last"
                            ).to_dict(orient="records")
                        except Exception as e:
                            print(
                                f"[{self.name}] Failed to deduplicate message_batch: {e}",
                                flush=True,
                            )
                            deduped_batch = message_batch
                        success = self.upload_to_iceberg(deduped_batch)
                        if success:
                            message_batch = []
                            last_upload_time = time.time()
                        else:
                            self._handle_upload_failure(
                                current_time, circuit_breaker_pause_until, deduped_batch
                            )
                    continue

                # 메시지를 받았을 때 로그 출력
                total_records = sum(
                    len(records) for records in message_records.values()
                )
                print(
                    f"[{self.name}] 📨 Received {total_records} messages from Kafka",
                    flush=True,
                )

                for topic_partition, records in message_records.items():
                    for record in records:
                        if self.stop_event.is_set() or stop_event.is_set():
                            break
                        # Skip records with None values (failed deserialization)
                        if record.value is not None:
                            message_batch.append(record.value)
                        else:
                            print(
                                f"[{self.name}] Skipping record with None value (deserialization failed)",
                                flush=True,
                            )

                    if self.stop_event.is_set() or stop_event.is_set():
                        break

                # Check if batch is full or time limit reached
                batch_full = len(message_batch) >= S3_BATCH_MAX_RECORDS
                time_limit_reached = message_batch and (
                    time.time() - last_upload_time >= S3_BATCH_MAX_SECONDS
                )

                if batch_full or time_limit_reached:
                    if message_batch:
                        # 중복 제거: primary key 기준
                        try:
                            df = pd.DataFrame(message_batch)
                            deduped_batch = df.drop_duplicates(
                                subset=self.primary_key, keep="last"
                            ).to_dict(orient="records")
                        except Exception as e:
                            print(
                                f"[{self.name}] Failed to deduplicate message_batch: {e}",
                                flush=True,
                            )
                            deduped_batch = message_batch
                        print(
                            f"[{self.name}] {'Record limit' if batch_full else 'Time limit'} reached for batch. Uploading {len(deduped_batch)} unique records (from {len(message_batch)} records).",
                            flush=True,
                        )
                        success = self.upload_to_iceberg(deduped_batch)
                        if success:
                            message_batch = []
                            last_upload_time = time.time()
                        else:
                            self._handle_upload_failure(
                                current_time, circuit_breaker_pause_until, deduped_batch
                            )

                if self.stop_event.is_set() or stop_event.is_set():
                    break

            # Upload any remaining messages in the batch
            if message_batch:
                # 중복 제거: primary key 기준
                try:
                    df = pd.DataFrame(message_batch)
                    deduped_batch = df.drop_duplicates(
                        subset=self.primary_key, keep="last"
                    ).to_dict(orient="records")
                except Exception as e:
                    print(
                        f"[{self.name}] Failed to deduplicate message_batch: {e}",
                        flush=True,
                    )
                    deduped_batch = message_batch
                print(
                    f"[{self.name}] Stop event received. Uploading remaining {len(deduped_batch)} unique records in batch (from {len(message_batch)} records).",
                    flush=True,
                )
                self.upload_to_iceberg(deduped_batch)

        except Exception as e:
            if hasattr(e, "errno") and e.errno == -1:
                print(
                    f"[{self.name}] Kafka consumer connection closed as expected.",
                    flush=True,
                )
            else:
                error_msg = f"Critical error in Kafka to Iceberg consumer job: {e}"
                print(
                    f"[{self.name}] Error in Kafka to Iceberg consumer job: {e}",
                    flush=True,
                )
                self.send_limited_slack_alert(
                    error_msg, "kafka_consumer_critical_error", message_type="error"
                )
        finally:
            print(
                f"[{self.name}] Kafka to Iceberg consumer job stopping...", flush=True
            )
            if "kafka_consumer" in locals() and kafka_consumer:
                kafka_consumer.close()
                print(f"[{self.name}] Kafka consumer closed in job.", flush=True)

    def _handle_upload_failure(
        self, current_time, circuit_breaker_pause_until, message_batch
    ):
        """Handle upload failure with circuit breaker logic"""
        cb_status = self.circuit_breaker.get_status()
        print(
            f"[{self.name}] Upload failed. Circuit breaker: {cb_status['state']}, failures: {cb_status['consecutive_failures']}",
            flush=True,
        )

        if self.circuit_breaker.state == CircuitBreakerState.OPEN:
            circuit_breaker_pause_until = (
                current_time + self.circuit_breaker.recovery_timeout
            )
            print(f"[{self.name}] Circuit breaker OPEN - pausing consumer", flush=True)

            # 🆕 제한된 Slack 알림으로 변경 (스팸 방지)
            self.send_limited_slack_alert(
                f"⚠️ Consumer paused due to circuit breaker OPEN\n"
                f"• Failed batch size: {len(message_batch)} records\n"
                f"• Consecutive failures: {cb_status['consecutive_failures']}\n"
                f"• Pause duration: {self.circuit_breaker.recovery_timeout}s",
                "circuit_breaker_open",
                message_type="warning",
            )

            # Check if we should stop producer
            if self.stop_producer_on_iceberg_failure:
                print(
                    f"[{self.name}] Stopping producer due to Iceberg failure (configured option)",
                    flush=True,
                )
                self.send_limited_slack_alert(
                    f"🛑 Stopping producer due to persistent Iceberg failures",
                    "producer_stopped_iceberg_failure",
                    message_type="warning",
                )
                self.stop_event.set()

    def get_circuit_breaker_status(self) -> dict:
        """Get circuit breaker status for external monitoring"""
        return self.circuit_breaker.get_status()

    def get_processing_lag_info(self) -> dict:
        """
        처리 지연 정보 반환 (단순화된 버전)
        """
        try:
            # MySQL에서 현재 최대 ID 조회
            conn = self.get_mysql_connection()
            if conn:
                mysql_max_id = self.get_max_id_from_mysql(conn)
                conn.close()
            else:
                mysql_max_id = 0

            total_lag = mysql_max_id - self.last_processed_id  # 전체 지연

            return {
                "mysql_max_id": mysql_max_id,
                "last_processed_id": self.last_processed_id,
                "total_lag": total_lag,  # 전체 지연
            }
        except Exception as e:
            print(f"[{self.name}] Failed to get processing lag info: {e}", flush=True)
            return {
                "error": str(e),
                "mysql_max_id": 0,
                "last_processed_id": self.last_processed_id,
                "total_lag": 0,
            }

    def _update_pipeline_status_file(self):
        """Update the pipeline_status.json file with the latest last_processed_id"""
        try:
            import os
            import json

            status_file = PIPELINE_STATUS_FILE
            if os.path.exists(status_file):
                with open(status_file, "r") as f:
                    status_data = json.load(f)

                if self.name in status_data:
                    status_data[self.name]["last_processed_id"] = self.last_processed_id
                    with open(status_file, "w") as f:
                        json.dump(status_data, f)
                    print(
                        f"[{self.name}] 📋 Updated last_processed_id in status file: {self.last_processed_id}",
                        flush=True,
                    )
                else:
                    print(
                        f"[{self.name}] 📋 Pipeline not found in status file (first time run)",
                        flush=True,
                    )
            else:
                print(
                    f"[{self.name}] 📋 No status file found (first time run)",
                    flush=True,
                )

        except Exception as e:
            print(
                f"[{self.name}] ⚠️ Failed to update pipeline_status.json: {e}",
                flush=True,
            )
