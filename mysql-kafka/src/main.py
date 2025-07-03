#!/usr/bin/env python3
"""
MySQL → Kafka → S3 파이프라인 메인 실행 스크립트
"""

import signal
import sys
import threading
import time
import os
from datetime import datetime

# 작업 디렉토리를 mysql-kafka 루트로 변경
script_dir = os.path.dirname(os.path.abspath(__file__))
mysql_kafka_root = os.path.dirname(script_dir)
os.chdir(mysql_kafka_root)

# 상위 디렉토리를 Python path에 추가
sys.path.append(mysql_kafka_root)

# config 파일에서 설정 가져오기
try:
    from src.config import PIPELINES, PIPELINE_COMMAND_FILE
except ImportError:
    print(
        "❌ Error: config.py file not found. Please ensure config.py exists in the src directory."
    )
    sys.exit(1)

from src.pipeline_manager import PipelineManager


def signal_handler(signum, frame):
    """시그널 핸들러 - 우아한 종료"""
    print(f"\n🛑 Received signal {signum}. Shutting down gracefully...")
    global_stop_event.set()


def main():
    """메인 함수"""
    global global_stop_event
    global_stop_event = threading.Event()

    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("🚀 Starting MySQL → Kafka → S3 Pipeline Manager")
    print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Working directory: {os.getcwd()}")

    try:
        # 파이프라인 매니저 생성
        pipeline_manager = PipelineManager(PIPELINES, global_stop_event)

        # 모든 파이프라인 시작
        pipeline_manager.start_all_pipelines()

        # 모니터링 스레드 시작
        monitor_thread = threading.Thread(
            target=pipeline_manager.monitor_pipelines,
            name="pipeline_monitor",
            daemon=True,
        )
        monitor_thread.start()

        # 외부 명령 처리 스레드 시작
        command_thread = threading.Thread(
            target=process_external_commands,
            args=(pipeline_manager, global_stop_event),
            name="command_processor",
            daemon=True,
        )
        command_thread.start()

        # 메인 루프 - 종료 신호 대기
        while not global_stop_event.is_set():
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
    finally:
        print("🛑 Shutting down all pipelines...")
        global_stop_event.set()

        if "pipeline_manager" in locals():
            pipeline_manager.stop_all_pipelines()

        print("✅ Pipeline manager stopped gracefully")
        print(f"📅 Stopped at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def process_external_commands(
    pipeline_manager: PipelineManager, stop_event: threading.Event
):
    """외부 명령 처리 (파일 기반)"""
    command_file = PIPELINE_COMMAND_FILE

    while not stop_event.is_set():
        try:
            if os.path.exists(command_file):
                with open(command_file, "r") as f:
                    command = f.read().strip()

                # 명령 파일 삭제
                os.remove(command_file)

                # 명령 처리
                parts = command.split()
                if len(parts) >= 2:
                    action = parts[0].lower()
                    pipeline_name = parts[1]

                    if action == "restart":
                        print(
                            f"📝 Processing restart command for pipeline: {pipeline_name}"
                        )
                        pipeline_manager.restart_pipeline(pipeline_name)
                    elif action == "stop":
                        print(
                            f"📝 Processing stop command for pipeline: {pipeline_name}"
                        )
                        pipeline_manager.stop_pipeline(pipeline_name)
                    else:
                        print(f"⚠️ Unknown command: {command}")
                else:
                    print(f"⚠️ Invalid command format: {command}")

        except Exception as e:
            print(f"⚠️ Error processing external command: {e}")

        # 1초 대기
        time.sleep(1)


if __name__ == "__main__":
    main()
