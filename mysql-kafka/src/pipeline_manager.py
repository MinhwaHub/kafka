#!/usr/bin/env python3
"""
MySQL → Kafka → S3 파이프라인 관리 스크립트
실행 중인 파이프라인을 외부에서 제어할 수 있는 독립적인 관리 도구
"""

import argparse
import json
import time
import signal
import os
import sys
from datetime import datetime
import psutil
import threading
from typing import Dict, List

# src 디렉토리에서 실행하는 경우 상위 디렉토리를 Python path에 추가
if os.path.basename(os.getcwd()) == "src":
    parent_dir = os.path.dirname(os.getcwd())
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

# config 파일에서 설정 가져오기
try:
    # src 디렉토리에서 실행하는 경우
    from config import (
        PIPELINES,
        PIPELINE_MANAGER_LOG,
        MAIN_PIPELINE_PID_FILE,
        PIPELINE_STDOUT_LOG,
        PIPELINE_STDERR_LOG,
        MAIN_SCRIPT_PATH,
        PIPELINE_STATUS_FILE,
        PIPELINE_COMMAND_FILE,
        SLACK_TOKEN,
        SLACK_CHANNEL_ID,
    )
except ImportError:
    try:
        # mysql-kafka 루트에서 실행하는 경우
        from src.config import (
            PIPELINES,
            PIPELINE_MANAGER_LOG,
            MAIN_PIPELINE_PID_FILE,
            PIPELINE_STDOUT_LOG,
            PIPELINE_STDERR_LOG,
            MAIN_SCRIPT_PATH,
            PIPELINE_STATUS_FILE,
            PIPELINE_COMMAND_FILE,
            SLACK_TOKEN,
            SLACK_CHANNEL_ID,
        )
    except ImportError:
        print(
            "❌ Error: config.py file not found. Please ensure config.py exists in the src directory."
        )
        sys.exit(1)

# pipeline import도 적응적으로 처리
try:
    # src 디렉토리에서 실행하는 경우
    from pipeline import MySQLKafkaS3Pipeline
except ImportError:
    try:
        # mysql-kafka 루트에서 실행하는 경우
        from src.pipeline import MySQLKafkaS3Pipeline
    except ImportError:
        print("❌ Error: pipeline.py file not found.")
        sys.exit(1)

from utils.circuit_breaker import CircuitBreakerState
from utils.slack_util import SlackMessenger


class PipelineManagerCLI:
    """
    파이프라인 관리 CLI 도구
    cmd에 따른 절차 및 로그 출력
    """

    def __init__(self):
        self.log_file = PIPELINE_MANAGER_LOG
        self.pid_file = MAIN_PIPELINE_PID_FILE  # 실행중인 파이프라인 프로세스 ID

        # Slack messenger 초기화 (App 기반)
        self.slack_messenger = SlackMessenger(
            token=SLACK_TOKEN,
            channel_id=SLACK_CHANNEL_ID,
        )

    def log_message(self, message: str):
        """로그 메시지 출력 및 파일 저장"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)

        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(log_entry + "\n")
        except Exception as e:
            print(f"⚠️ Failed to write to log file: {e}")

    def is_pipeline_running(self):
        """파이프라인 프로세스가 실행 중인지 확인"""
        if not os.path.exists(self.pid_file):
            return False, None

        try:
            with open(self.pid_file, "r") as f:
                pid = int(f.read().strip())

            # PID가 실제로 실행 중인지 확인
            if psutil.pid_exists(pid):
                process = psutil.Process(pid)

                # 프로세스 명령줄 인자를 확인해서 main.py가 포함되어 있는지 체크
                try:
                    cmdline = process.cmdline()
                    # cmdline은 ['python', 'main.py'] 형태
                    if len(cmdline) >= 2 and "main.py" in cmdline[1]:
                        return True, pid
                except (psutil.AccessDenied, psutil.NoSuchProcess):
                    pass

            # PID 파일이 있지만 올바른 프로세스가 아니면 파일 삭제
            os.remove(self.pid_file)
            return False, None

        except Exception as e:
            self.log_message(f"⚠️ Error checking pipeline status: {e}")
            return False, None

    def start_pipeline(self):
        """파이프라인 시작"""
        is_running, pid = self.is_pipeline_running()

        if is_running:
            self.log_message(f"⚠️ Pipeline is already running (PID: {pid})")
            return False

        self.log_message("🚀 Starting pipeline...")

        # Slack 알림: 파이프라인 시작 (상태 정보 포함)
        status_data = self.get_structured_status()
        status_blocks = self.format_status_for_slack(status_data)
        self.slack_messenger.send_slack(
            text="🚀 *Pipeline is starting...*",
            # block_message=status_blocks,
            color="#36a64f",
        )

        try:
            # 백그라운드에서 파이프라인 시작
            import subprocess
            import sys

            # Python 경로와 main.py 경로 확인
            python_path = sys.executable
            main_script = MAIN_SCRIPT_PATH

            if not os.path.exists(main_script):
                self.log_message(
                    f"❌ Error: {main_script} not found in current directory"
                )
                self.send_slack_notification(
                    f"❌ Pipeline start failed: {main_script} not found", "error"
                )
                return False

            # nohup으로 백그라운드 실행
            with open(PIPELINE_STDOUT_LOG, "w") as stdout_file, open(
                PIPELINE_STDERR_LOG, "w"
            ) as stderr_file:
                process = subprocess.Popen(
                    [python_path, main_script],
                    stdout=stdout_file,
                    stderr=stderr_file,
                    start_new_session=True,
                )

                # PID 저장
                with open(self.pid_file, "w") as f:
                    f.write(str(process.pid))

                self.log_message(
                    f"✅ Pipeline started successfully (PID: {process.pid})"
                )
                self.log_message(
                    f"📋 Logs: {PIPELINE_STDOUT_LOG} (stdout), {PIPELINE_STDERR_LOG} (stderr)"
                )

                # Slack 알림: 파이프라인 시작 성공 (업데이트된 상태 정보 포함)
                time.sleep(1)  # 프로세스가 완전히 시작될 때까지 잠시 대기
                updated_status_data = self.get_structured_status()
                updated_status_blocks = self.format_status_for_slack(
                    updated_status_data
                )
                self.slack_messenger.send_slack(
                    text=f"✅ *Pipeline started successfully (PID: {process.pid})*",
                    block_message=updated_status_blocks,
                    color="#36a64f",
                )

                return True

        except Exception as e:
            self.log_message(f"❌ Failed to start pipeline: {e}")
            self.slack_messenger.send_slack(
                text=f"❌ *Failed to start pipeline: {e}*", color="#ff0000"
            )
            return False

    def stop_pipeline(self):
        """파이프라인 중지"""
        is_running, pid = self.is_pipeline_running()

        if not is_running:
            self.log_message("⚠️ Pipeline is not running")
            return False

        try:
            self.log_message(f"🛑 Stopping pipeline (PID: {pid})...")

            # Slack 알림: 파이프라인 중지 시작 (현재 상태 정보 포함)
            status_data = self.get_structured_status()
            status_blocks = self.format_status_for_slack(status_data)
            self.slack_messenger.send_slack(
                text=f"🛑 *Stopping pipeline (PID: {pid})...*",
                # block_message=status_blocks,
                color="#ffa500",
            )

            process = psutil.Process(pid)

            # SIGTERM으로 우아한 종료 시도
            process.terminate()

            # 최대 10초 대기
            try:
                process.wait(timeout=10)
                self.log_message("✅ Pipeline stopped gracefully")
                # Slack 알림: 파이프라인 정상 중지 (업데이트된 상태 정보 포함)
                updated_status_data = self.get_structured_status()
                updated_status_blocks = self.format_status_for_slack(
                    updated_status_data
                )
                self.slack_messenger.send_slack(
                    text="✅ *Pipeline stopped gracefully*",
                    block_message=updated_status_blocks,
                    color="#36a64f",
                )
            except psutil.TimeoutExpired:
                # 강제 종료
                self.log_message("⚠️ Graceful shutdown timeout. Force killing...")
                process.kill()
                process.wait()
                self.log_message("✅ Pipeline force stopped")
                # Slack 알림: 파이프라인 강제 중지 (업데이트된 상태 정보 포함)
                updated_status_data = self.get_structured_status()
                updated_status_blocks = self.format_status_for_slack(
                    updated_status_data
                )
                self.slack_messenger.send_slack(
                    text="⚠️ *Pipeline force stopped (timeout)*",
                    block_message=updated_status_blocks,
                    color="#ffa500",
                )

            # PID 파일 삭제
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)

            # 런타임 상태 파일 업데이트 (모든 파이프라인을 중지 상태로 설정)
            self.update_status_on_shutdown()

            return True

        except Exception as e:
            self.log_message(f"❌ Failed to stop pipeline: {e}")
            self.slack_messenger.send_slack(
                text=f"❌ *Failed to stop pipeline: {e}*", color="#ff0000"
            )
            return False

    def update_status_on_shutdown(self):
        """전체 파이프라인 종료시 상태 파일 업데이트"""
        try:
            status_file = PIPELINE_STATUS_FILE

            # 기존 상태 파일이 있으면 로드, 없으면 새로 생성
            status_data = {}
            if os.path.exists(status_file):
                try:
                    with open(status_file, "r") as f:
                        status_data = json.load(f)
                except:
                    pass

            # 모든 설정된 파이프라인을 중지 상태로 업데이트
            for pipeline in PIPELINES:
                pipeline_name = pipeline["name"]

                # 기존 데이터 보존 (재시작 횟수, last_processed_id 등)
                if pipeline_name in status_data:
                    restart_count = status_data[pipeline_name].get("restart_count", 0)
                    last_processed_id = status_data[pipeline_name].get(
                        "last_processed_id", 0
                    )
                    # 기존 is_failed 상태도 보존 (정상 종료라면 false 유지)
                    current_is_failed = status_data[pipeline_name].get(
                        "is_failed", False
                    )
                else:
                    restart_count = 0
                    last_processed_id = 0
                    current_is_failed = False

                status_data[pipeline_name] = {
                    "enabled": False,  # 종료되었으므로 비활성화
                    "is_failed": current_is_failed,  # 기존 상태 보존
                    "restart_count": restart_count,
                    "last_processed_id": last_processed_id,  # 보존
                    "table_name": pipeline["mysql"]["table"],
                    "kafka_topic": pipeline["kafka"]["topic"],
                }

            # 업데이트된 상태를 파일에 저장
            with open(status_file, "w") as f:
                json.dump(status_data, f, indent=2)

            self.log_message("📝 Pipeline status file updated on shutdown")

        except Exception as e:
            self.log_message(f"⚠️ Failed to update status file on shutdown: {e}")

    def update_pipeline_success_status(
        self, pipeline_name: str, last_processed_id: int = None
    ):
        """파이프라인 성공 상태 업데이트 (실시간)"""
        try:
            status_file = PIPELINE_STATUS_FILE

            # 기존 상태 파일 로드
            status_data = {}
            if os.path.exists(status_file):
                try:
                    with open(status_file, "r") as f:
                        status_data = json.load(f)
                except:
                    pass

            # 해당 파이프라인 찾기
            pipeline_config = None
            for pipeline in PIPELINES:
                if pipeline["name"] == pipeline_name:
                    pipeline_config = pipeline
                    break

            if not pipeline_config:
                return

            # 성공 상태로 업데이트
            if pipeline_name not in status_data:
                status_data[pipeline_name] = {}

            current_data = status_data[pipeline_name]

            status_data[pipeline_name] = {
                "enabled": True,  # 실행 중
                "is_failed": False,  # 성공 상태
                "restart_count": current_data.get("restart_count", 0),
                "last_processed_id": (
                    last_processed_id
                    if last_processed_id is not None
                    else current_data.get("last_processed_id", 0)
                ),
                "table_name": pipeline_config["mysql"]["table"],
                "kafka_topic": pipeline_config["kafka"]["topic"],
            }

            # 파일 저장
            with open(status_file, "w") as f:
                json.dump(status_data, f, indent=2)

        except Exception as e:
            self.log_message(f"⚠️ Failed to update pipeline success status: {e}")

    def restart_pipeline(self):
        """파이프라인 재시작"""
        self.log_message("🔄 Restarting pipeline...")

        # 먼저 중지
        self.stop_pipeline()

        # 잠시 대기
        time.sleep(3)

        # 다시 시작
        return self.start_pipeline()

    def status_pipeline(self):
        """파이프라인 상태 확인"""
        is_running, pid = self.is_pipeline_running()

        self.log_message("📊 Pipeline Status:")

        if is_running:
            try:
                process = psutil.Process(pid)
                create_time = datetime.fromtimestamp(process.create_time())
                memory_info = process.memory_info()
                cpu_percent = process.cpu_percent()

                self.log_message(f"  Status: ✅ RUNNING")
                self.log_message(f"  PID: {pid}")
                self.log_message(
                    f"  Started: {create_time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                self.log_message(f"  Memory: {memory_info.rss / 1024 / 1024:.1f} MB")
                self.log_message(f"  CPU: {cpu_percent}%")

                # 자식 프로세스 (스레드) 개수
                try:
                    children = process.children()
                    self.log_message(f"  Child Processes: {len(children)}")
                except:
                    pass

            except Exception as e:
                self.log_message(f"  Status: ❌ ERROR - {e}")
        else:
            self.log_message(f"  Status: ❌ NOT RUNNING")

        # 런타임 상태 파일 로드
        runtime_status = {}
        status_file = PIPELINE_STATUS_FILE
        try:
            if os.path.exists(status_file):
                with open(status_file, "r") as f:
                    runtime_status = json.load(f)
        except Exception as e:
            self.log_message(f"⚠️ Warning: Could not load runtime status: {e}")

        # 설정된 파이프라인 정보
        self.log_message(f"\n📋 Configured Pipelines ({len(PIPELINES)}):")
        for i, pipeline in enumerate(PIPELINES, 1):
            pipeline_name = pipeline["name"]

            # 런타임 상태가 있으면 우선 사용, 없으면 config 기본값 사용
            if pipeline_name in runtime_status:
                runtime_info = runtime_status[pipeline_name]
                is_enabled = runtime_info.get("enabled", True)
                is_failed = runtime_info.get("is_failed", False)
                restart_count = runtime_info.get("restart_count", 0)

                # 상태 아이콘 결정
                if is_failed:
                    status_icon = "❌"
                    status_text = " (FAILED)"
                elif not is_enabled:
                    status_icon = "⏸️"
                    status_text = " (STOPPED)"
                else:
                    status_icon = "✅"
                    status_text = ""

                # 재시작 횟수 표시
                restart_info = (
                    f" (restarts: {restart_count})" if restart_count > 0 else ""
                )

            else:
                # 런타임 상태가 없으면 config 기본값 사용
                is_enabled = pipeline.get("enabled", True)
                status_icon = "✅" if is_enabled else "❌"
                status_text = " (config default)"
                restart_info = ""

            self.log_message(
                f"  {i}. {status_icon} {pipeline_name}{status_text}{restart_info}"
            )
            self.log_message(f"     Table: {pipeline['mysql']['table']}")
            self.log_message(f"     Topic: {pipeline['kafka']['topic']}")

        # 로그 파일 정보
        self.log_message(f"\n📄 Log Files:")
        for log_file in [PIPELINE_STDOUT_LOG, PIPELINE_STDERR_LOG, self.log_file]:
            if os.path.exists(log_file):
                stat = os.stat(log_file)
                size = stat.st_size / 1024  # KB
                modified = datetime.fromtimestamp(stat.st_mtime)
                self.log_message(
                    f"  {log_file}: {size:.1f} KB (modified: {modified.strftime('%Y-%m-%d %H:%M:%S')})"
                )
            else:
                self.log_message(f"  {log_file}: Not found")

    def show_logs(self, lines: int = 50):
        """최근 로그 보기"""
        log_files = {
            "stdout": PIPELINE_STDOUT_LOG,
            "stderr": PIPELINE_STDERR_LOG,
            "manager": self.log_file,
        }

        for log_type, log_file in log_files.items():
            if os.path.exists(log_file):
                self.log_message(
                    f"\n📄 {log_type.upper()} - Last {lines} lines from {log_file}:"
                )
                try:
                    with open(log_file, "r", encoding="utf-8") as f:
                        all_lines = f.readlines()
                        recent_lines = (
                            all_lines[-lines:] if len(all_lines) > lines else all_lines
                        )

                        for line in recent_lines:
                            print(f"  {line.rstrip()}")

                except Exception as e:
                    self.log_message(f"⚠️ Error reading {log_file}: {e}")
            else:
                self.log_message(f"\n📄 {log_type.upper()}: {log_file} not found")

    def restart_single_pipeline(self, pipeline_name: str):
        """개별 파이프라인 재시작"""
        is_running, pid = self.is_pipeline_running()

        if not is_running:
            self.log_message(
                "❌ Pipeline is not running. Cannot restart individual pipeline."
            )
            return False

        # 개별 파이프라인 재시작 명령 파일 생성
        command_file = PIPELINE_COMMAND_FILE
        command = f"restart {pipeline_name}"

        try:
            with open(command_file, "w") as f:
                f.write(command)

            self.log_message(f"🔄 Restart command sent for pipeline '{pipeline_name}'")
            self.log_message(f"📝 Command file created: {command_file}")

            # 잠시 대기 후 명령 파일이 처리되었는지 확인
            max_wait = 10  # 10초 대기
            for i in range(max_wait):
                time.sleep(1)
                if not os.path.exists(command_file):
                    self.log_message(f"✅ Command processed successfully")
                    return True

            self.log_message(
                f"⚠️ Command file still exists after {max_wait}s. Check pipeline logs."
            )
            return False

        except Exception as e:
            self.log_message(f"❌ Failed to send restart command: {e}")
            return False

    def stop_single_pipeline(self, pipeline_name: str):
        """개별 파이프라인 중지"""
        is_running, pid = self.is_pipeline_running()

        if not is_running:
            self.log_message(
                "❌ Pipeline is not running. Cannot stop individual pipeline."
            )
            return False

        # 개별 파이프라인 중지 명령 파일 생성
        command_file = PIPELINE_COMMAND_FILE
        command = f"stop {pipeline_name}"

        try:
            with open(command_file, "w") as f:
                f.write(command)

            self.log_message(f"🛑 Stop command sent for pipeline '{pipeline_name}'")
            self.log_message(f"📝 Command file created: {command_file}")

            # 잠시 대기 후 명령 파일이 처리되었는지 확인
            max_wait = 10  # 10초 대기
            for i in range(max_wait):
                time.sleep(1)
                if not os.path.exists(command_file):
                    self.log_message(f"✅ Command processed successfully")
                    return True

            self.log_message(
                f"⚠️ Command file still exists after {max_wait}s. Check pipeline logs."
            )
            return False

        except Exception as e:
            self.log_message(f"❌ Failed to send stop command: {e}")
            return False

    def send_slack_notification(self, message: str, message_type: str = "info"):
        """Slack 알림 전송"""
        try:
            if message_type == "error":
                color = "#ff0000"  # 빨간색
                alert_message = f"🚨 *Pipeline Manager Alert*"
            elif message_type == "warning":
                color = "#ffa500"  # 주황색
                alert_message = f"*Pipeline Manager Warning*"
            else:  # info
                color = "#36a64f"  # 초록색
                alert_message = f"*Pipeline Manager Info*"

            alert_message += f"\n{message}"

            self.slack_messenger.send_slack(text=alert_message, color=color)
            self.log_message(f"📨 Slack notification sent: {message}")
        except Exception as e:
            self.log_message(f"⚠️ Failed to send Slack notification: {e}")

    def get_structured_status(self) -> dict:
        """파이프라인 상태를 구조화된 데이터로 반환"""
        is_running, pid = self.is_pipeline_running()

        status_data = {
            "pipeline_running": is_running,
            "pid": pid,
            "pipelines": [],
            "log_files": [],
        }

        # 프로세스 정보
        if is_running:
            try:
                process = psutil.Process(pid)
                create_time = datetime.fromtimestamp(process.create_time())
                memory_info = process.memory_info()
                cpu_percent = process.cpu_percent()

                status_data["process_info"] = {
                    "started": create_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "memory_mb": round(memory_info.rss / 1024 / 1024, 1),
                    "cpu_percent": cpu_percent,
                }

                try:
                    children = process.children()
                    status_data["process_info"]["child_processes"] = len(children)
                except:
                    status_data["process_info"]["child_processes"] = 0

            except Exception:
                status_data["process_info"] = {"error": "Failed to get process info"}

        # 런타임 상태 파일 로드
        runtime_status = {}
        try:
            if os.path.exists(PIPELINE_STATUS_FILE):
                with open(PIPELINE_STATUS_FILE, "r") as f:
                    runtime_status = json.load(f)
        except Exception:
            pass

        # 파이프라인 정보
        for i, pipeline in enumerate(PIPELINES, 1):
            pipeline_name = pipeline["name"]
            pipeline_info = {
                "name": pipeline_name,
                "table": pipeline["mysql"]["table"],
                "topic": pipeline["kafka"]["topic"],
            }

            if pipeline_name in runtime_status:
                runtime_info = runtime_status[pipeline_name]
                is_enabled = runtime_info.get("enabled", True)
                is_failed = runtime_info.get("is_failed", False)
                restart_count = runtime_info.get("restart_count", 0)

                if is_failed:
                    pipeline_info["status"] = "FAILED"
                    pipeline_info["status_icon"] = "❌"
                elif not is_enabled:
                    pipeline_info["status"] = "STOPPED"
                    pipeline_info["status_icon"] = "⏸️"
                else:
                    pipeline_info["status"] = "RUNNING"
                    pipeline_info["status_icon"] = "✅"

                pipeline_info["restart_count"] = restart_count
            else:
                is_enabled = pipeline.get("enabled", True)
                pipeline_info["status"] = "RUNNING" if is_enabled else "DISABLED"
                pipeline_info["status_icon"] = "✅" if is_enabled else "❌"
                pipeline_info["restart_count"] = 0

            status_data["pipelines"].append(pipeline_info)

        # 로그 파일 정보
        for log_name, log_file in [
            ("stdout", PIPELINE_STDOUT_LOG),
            ("stderr", PIPELINE_STDERR_LOG),
            ("manager", self.log_file),
        ]:
            if os.path.exists(log_file):
                stat = os.stat(log_file)
                size_kb = round(stat.st_size / 1024, 1)
                modified = datetime.fromtimestamp(stat.st_mtime)
                status_data["log_files"].append(
                    {
                        "name": log_name,
                        "path": log_file,
                        "size_kb": size_kb,
                        "modified": modified.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
            else:
                status_data["log_files"].append(
                    {"name": log_name, "path": log_file, "exists": False}
                )

        return status_data

    def format_status_for_slack(self, status_data: dict) -> list:
        """구조화된 상태 데이터를 Slack block 형태로 변환"""
        blocks = []

        # 메인 상태
        main_status = "🟢 RUNNING" if status_data["pipeline_running"] else "🔴 STOPPED"
        status_text = f"*Pipeline Status:* {main_status}"

        if status_data["pipeline_running"] and "process_info" in status_data:
            proc_info = status_data["process_info"]
            if "error" not in proc_info:
                status_text += f"\n• PID: {status_data['pid']}"
                status_text += f"\n• Started: {proc_info['started']}"
                status_text += f"\n• Memory: {proc_info['memory_mb']} MB"
                status_text += f"\n• Child Processes: {proc_info['child_processes']}"

        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": status_text}}
        )

        # 파이프라인 목록
        if status_data["pipelines"]:
            pipeline_text = (
                f"*Configured Pipelines ({len(status_data['pipelines'])}):*\n"
            )
            for i, pipeline in enumerate(status_data["pipelines"], 1):
                restart_info = (
                    f" (restarts: {pipeline['restart_count']})"
                    if pipeline["restart_count"] > 0
                    else ""
                )
                pipeline_text += (
                    f"{i}. {pipeline['status_icon']} {pipeline['name']}{restart_info}\n"
                )
                pipeline_text += f"   • Table: {pipeline['table']}\n"
                pipeline_text += f"   • Topic: {pipeline['topic']}\n"

            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": pipeline_text.strip()},
                }
            )

        # 로그 파일 정보 (간략화)
        log_text = "*Log Files:*\n"
        for log_file in status_data["log_files"]:
            if log_file.get("exists", True):
                log_text += f"• {log_file['name']}: {log_file['size_kb']} KB\n"
            else:
                log_text += f"• {log_file['name']}: Not found\n"

        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": log_text.strip()}}
        )

        return blocks


class PipelineManager:
    """파이프라인 관리자 클래스"""

    def __init__(
        self, pipelines_config: List[dict], global_stop_event: threading.Event
    ):
        """파이프라인 관리자 초기화"""
        self.global_stop_event = global_stop_event
        self.pipelines: Dict[str, MySQLKafkaS3Pipeline] = {}
        self.pipeline_threads: Dict[str, Dict[str, threading.Thread]] = {}
        self.pipeline_status: Dict[str, Dict] = {}
        self.restart_counts: Dict[str, int] = {}

        # 최대 재시작 시도 횟수 설정 (첫 번째 파이프라인 설정에서 가져오거나 기본값 사용)
        self.max_restart_attempts: int = 10
        if pipelines_config:
            self.max_restart_attempts = pipelines_config[0].get(
                "max_restart_attempts", 10
            )

        # 실패한 파이프라인 추적 (재시작 제한 도달)
        self.failed_pipelines: set = set()

        # Initialize pipelines
        for pipeline_config in pipelines_config:
            pipeline_name = pipeline_config["name"]
            if pipeline_config.get("enabled", True):
                pipeline = MySQLKafkaS3Pipeline(pipeline_config, global_stop_event)
                self.pipelines[pipeline_name] = pipeline
                self.pipeline_threads[pipeline_name] = {}
                self.pipeline_status[pipeline_name] = {
                    "status": "stopped",
                    "last_update": datetime.now(),
                    "producer_running": False,
                    "consumer_running": False,
                }
                self.restart_counts[pipeline_name] = 0
                print(f"✅ Pipeline '{pipeline_name}' registered")
            else:
                print(f"⏸️ Pipeline '{pipeline_name}' disabled in config")

        print(f"🔧 Max restart attempts per pipeline: {self.max_restart_attempts}")

    def start_pipeline(self, pipeline_name: str) -> bool:
        """개별 파이프라인 시작"""
        if pipeline_name not in self.pipelines:
            print(f"❌ Pipeline '{pipeline_name}' not found")
            return False

        pipeline = self.pipelines[pipeline_name]

        try:
            # Start producer thread
            producer_thread = threading.Thread(
                target=pipeline.mysql_to_kafka_producer_job,
                name=f"{pipeline_name}_producer",
                daemon=True,
            )
            producer_thread.start()
            self.pipeline_threads[pipeline_name]["producer"] = producer_thread

            # Start consumer thread
            consumer_thread = threading.Thread(
                target=pipeline.kafka_to_iceberg_consumer_job,
                name=f"{pipeline_name}_consumer",
                daemon=True,
            )
            consumer_thread.start()
            self.pipeline_threads[pipeline_name]["consumer"] = consumer_thread

            # Update memory status
            self.pipeline_status[pipeline_name].update(
                {
                    "status": "running",
                    "last_update": datetime.now(),
                    "producer_running": True,
                    "consumer_running": True,
                }
            )

            # 🆕 파이프라인 시작 시 즉시 파일 상태를 success로 업데이트
            self.update_pipeline_file_status(
                pipeline_name, enabled=True, is_failed=False
            )

            print(f"🚀 Pipeline '{pipeline_name}' started successfully")
            return True

        except Exception as e:
            print(f"❌ Failed to start pipeline '{pipeline_name}': {e}")
            self.pipeline_status[pipeline_name].update(
                {
                    "status": "error",
                    "last_update": datetime.now(),
                    "error": str(e),
                }
            )
            # 🆕 시작 실패 시 파일 상태를 failed로 업데이트
            self.update_pipeline_file_status(
                pipeline_name, enabled=False, is_failed=True
            )
            return False

    def stop_pipeline(self, pipeline_name: str) -> bool:
        """개별 파이프라인 중지"""
        if pipeline_name not in self.pipelines:
            print(f"❌ Pipeline '{pipeline_name}' not found")
            return False

        try:
            pipeline = self.pipelines[pipeline_name]
            pipeline.stop_event.set()

            # Wait for threads to stop
            threads = self.pipeline_threads.get(pipeline_name, {})
            for thread_type, thread in threads.items():
                if thread and thread.is_alive():
                    print(
                        f"⏳ Waiting for {pipeline_name} {thread_type} thread to stop..."
                    )
                    thread.join(timeout=10)
                    if thread.is_alive():
                        print(
                            f"⚠️ {pipeline_name} {thread_type} thread did not stop gracefully"
                        )
                    else:
                        print(f"✅ {pipeline_name} {thread_type} thread stopped")

            # Clear threads
            self.pipeline_threads[pipeline_name] = {}

            # Reset stop event for potential restart
            pipeline.stop_event.clear()

            # Update memory status
            self.pipeline_status[pipeline_name].update(
                {
                    "status": "stopped",
                    "last_update": datetime.now(),
                    "producer_running": False,
                    "consumer_running": False,
                }
            )

            # 🆕 파이프라인 중지 시 파일 상태를 stopped로 업데이트
            self.update_pipeline_file_status(
                pipeline_name, enabled=False, is_failed=False
            )

            print(f"🛑 Pipeline '{pipeline_name}' stopped successfully")
            return True

        except Exception as e:
            print(f"❌ Failed to stop pipeline '{pipeline_name}': {e}")
            return False

    def update_pipeline_file_status(
        self,
        pipeline_name: str,
        enabled: bool,
        is_failed: bool,
        last_processed_id: int = None,
    ):
        """파이프라인 파일 상태 업데이트 (즉시 반영)"""
        try:
            status_file = PIPELINE_STATUS_FILE

            # 기존 상태 파일 로드
            status_data = {}
            if os.path.exists(status_file):
                try:
                    with open(status_file, "r") as f:
                        status_data = json.load(f)
                except:
                    pass

            # 해당 파이프라인 찾기
            pipeline_config = None
            for pipeline in PIPELINES:
                if pipeline["name"] == pipeline_name:
                    pipeline_config = pipeline
                    break

            if not pipeline_config:
                print(f"⚠️ Pipeline config not found for '{pipeline_name}'")
                return

            # 기존 데이터 보존
            current_data = status_data.get(pipeline_name, {})

            # 상태 업데이트
            status_data[pipeline_name] = {
                "enabled": enabled,
                "is_failed": is_failed,
                "restart_count": current_data.get("restart_count", 0),
                "last_processed_id": (
                    last_processed_id
                    if last_processed_id is not None
                    else current_data.get("last_processed_id", 0)
                ),
                "table_name": pipeline_config["mysql"]["table"],
                "kafka_topic": pipeline_config["kafka"]["topic"],
            }

            # 파일 저장
            with open(status_file, "w") as f:
                json.dump(status_data, f, indent=2)

            status_text = (
                "SUCCESS"
                if enabled and not is_failed
                else ("FAILED" if is_failed else "STOPPED")
            )
            print(
                f"📝 Pipeline '{pipeline_name}' file status updated to: {status_text}"
            )

        except Exception as e:
            print(f"⚠️ Failed to update pipeline file status: {e}")

    def restart_pipeline(self, pipeline_name: str) -> bool:
        """개별 파이프라인 재시작"""
        # 재시작 제한 확인
        current_restart_count = self.restart_counts.get(pipeline_name, 0)
        if current_restart_count >= self.max_restart_attempts:
            error_msg = f"Pipeline '{pipeline_name}' has exceeded maximum restart attempts ({self.max_restart_attempts}). Marking as permanently failed."
            print(f"❌ {error_msg}")
            self.failed_pipelines.add(pipeline_name)
            return False

        print(
            f"🔄 Restarting pipeline '{pipeline_name}'... (attempt {current_restart_count + 1}/{self.max_restart_attempts})"
        )

        if self.stop_pipeline(pipeline_name):
            time.sleep(2)  # Brief pause between stop and start
            if self.start_pipeline(pipeline_name):
                self.restart_counts[pipeline_name] = current_restart_count + 1
                print(
                    f"✅ Pipeline '{pipeline_name}' restarted successfully (restart count: {self.restart_counts[pipeline_name]})"
                )
                return True

        print(f"❌ Failed to restart pipeline '{pipeline_name}'")
        return False

    def start_all_pipelines(self) -> None:
        """모든 파이프라인 시작"""
        print("🚀 Starting all pipelines...")

        # 🆕 파이프라인 시작 시 실패 상태 및 재시작 카운트 초기화
        print("🔄 Resetting pipeline failure states and restart counts...")
        self.failed_pipelines.clear()
        self.restart_counts.clear()
        for pipeline_name in self.pipelines.keys():
            self.restart_counts[pipeline_name] = 0

        for pipeline_name in self.pipelines.keys():
            self.start_pipeline(pipeline_name)

        print("✅ All pipelines start sequence completed")

    def stop_all_pipelines(self) -> None:
        """모든 파이프라인 중지"""
        print("🛑 Stopping all pipelines...")

        for pipeline_name in self.pipelines.keys():
            self.stop_pipeline(pipeline_name)

        print("✅ All pipelines stopped")

    def get_pipeline_status(self, pipeline_name: str) -> Dict:
        """개별 파이프라인 상태 조회"""
        if pipeline_name not in self.pipelines:
            return {"error": f"Pipeline '{pipeline_name}' not found"}

        pipeline = self.pipelines[pipeline_name]
        threads = self.pipeline_threads.get(pipeline_name, {})

        # Check thread status
        producer_alive = threads.get("producer") and threads["producer"].is_alive()
        consumer_alive = threads.get("consumer") and threads["consumer"].is_alive()

        # Get circuit breaker status
        cb_status = pipeline.get_circuit_breaker_status()

        status = {
            "name": pipeline_name,
            "status": "running" if (producer_alive and consumer_alive) else "stopped",
            "producer_running": producer_alive,
            "consumer_running": consumer_alive,
            "last_processed_id": pipeline.last_processed_id,
            "restart_count": self.restart_counts.get(pipeline_name, 0),
            "circuit_breaker": cb_status,
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # 🆕 처리 지연 정보 추가
        try:
            lag_info = pipeline.get_processing_lag_info()
            status["lag_info"] = lag_info
        except Exception as e:
            print(f"⚠️ Failed to get lag info for {pipeline_name}: {e}")
            status["lag_info"] = {"error": str(e)}

        # Update internal status
        self.pipeline_status[pipeline_name] = status

        return status

    def get_all_pipeline_status(self) -> Dict[str, Dict]:
        """모든 파이프라인 상태 조회"""
        all_status = {}
        for pipeline_name in self.pipelines.keys():
            all_status[pipeline_name] = self.get_pipeline_status(pipeline_name)
        return all_status

    def print_pipeline_status(self) -> None:
        """파이프라인 상태를 콘솔에 출력"""
        print("\n" + "=" * 80)
        print("📊 PIPELINE STATUS REPORT")
        print("=" * 80)

        all_status = self.get_all_pipeline_status()

        if not all_status:
            print("❌ No pipelines configured")
            return

        for pipeline_name, status in all_status.items():
            print(f"\n🔧 Pipeline: {pipeline_name}")

            # 실패한 파이프라인 표시
            if pipeline_name in self.failed_pipelines:
                print(f"   Status: 💀 PERMANENTLY FAILED (Max restarts exceeded)")
            else:
                print(
                    f"   Status: {'🟢 RUNNING' if status['status'] == 'running' else '🔴 STOPPED'}"
                )

            print(f"   Producer: {'✅' if status['producer_running'] else '❌'}")
            print(f"   Consumer: {'✅' if status['consumer_running'] else '❌'}")
            print(f"   Last Processed ID: {status['last_processed_id']}")
            print(
                f"   Restart Count: {status['restart_count']}/{self.max_restart_attempts}"
            )

            # 🆕 처리 지연 정보 표시
            if "lag_info" in status and "error" not in status["lag_info"]:
                lag_info = status["lag_info"]
                print(f"   📊 Processing Lag Info:")
                print(f"      MySQL Max ID: {lag_info['mysql_max_id']}")
                print(f"      Last Processed ID: {lag_info['last_processed_id']}")
                print(f"      Total Lag: {lag_info['total_lag']} records")

                # 지연 상태에 따른 경고 표시
                if lag_info["total_lag"] > 10000:
                    print(f"      ⚠️ High lag detected!")

            # Circuit breaker status with enhanced display
            cb_status = status["circuit_breaker"]
            cb_state = cb_status["state"]

            if cb_state == CircuitBreakerState.CLOSED:
                cb_emoji = "🟢"
                cb_text = "CLOSED"
            elif cb_state == CircuitBreakerState.OPEN:
                cb_emoji = "🔴"
                cb_text = "OPEN"
            else:  # HALF_OPEN
                cb_emoji = "🟡"
                cb_text = "HALF_OPEN"

            print(f"   Circuit Breaker: {cb_emoji} {cb_text}")
            print(f"   Consecutive Failures: {cb_status['consecutive_failures']}")

            if cb_state == CircuitBreakerState.OPEN:
                print(f"   Recovery Time: {cb_status['time_until_recovery']:.1f}s")

            print(f"   Last Update: {status['last_update']}")

        print("\n" + "=" * 80)
        print("📋 CONFIGURATION")
        print("=" * 80)

        # Show configuration for first pipeline as example
        if self.pipelines:
            first_pipeline = next(iter(self.pipelines.values()))
            print(
                f"🔧 Stop Producer on Iceberg Failure: {first_pipeline.stop_producer_on_iceberg_failure}"
            )
            print(
                f"📊 Max Kafka Lag Tolerance: {first_pipeline.max_kafka_lag_tolerance}"
            )
            print(f"🔄 Max Restart Attempts: {self.max_restart_attempts}")

        print("=" * 80)

    def monitor_pipelines(self) -> None:
        """파이프라인 모니터링 (백그라운드에서 실행)"""
        print("👁️ Starting pipeline monitoring...")

        while not self.global_stop_event.is_set():
            try:
                # 모든 파이프라인이 permanently failed 상태인지 확인
                if (
                    len(self.failed_pipelines) == len(self.pipelines)
                    and len(self.pipelines) > 0
                ):
                    print(
                        f"💀 All pipelines have permanently failed! Shutting down entire process..."
                    )
                    print(f"   Failed pipelines: {', '.join(self.failed_pipelines)}")
                    print(
                        "   Use 'python pipeline_manager.py restart' to reset and try again"
                    )
                    # 전체 프로세스 종료를 위해 global_stop_event 설정
                    self.global_stop_event.set()
                    break

                # Check each pipeline
                for pipeline_name in self.pipelines.keys():
                    if self.global_stop_event.is_set():
                        break

                    # 실패한 파이프라인은 재시작 시도하지 않음
                    if pipeline_name in self.failed_pipelines:
                        continue

                    status = self.get_pipeline_status(pipeline_name)

                    # Check if pipeline should be running but isn't
                    if status["status"] == "stopped":
                        print(
                            f"⚠️ Pipeline '{pipeline_name}' is stopped - attempting restart"
                        )
                        if not self.restart_pipeline(pipeline_name):
                            print(
                                f"❌ Failed to restart pipeline '{pipeline_name}' - monitoring disabled for this pipeline"
                            )

                # 실패한 파이프라인이 있지만 모든 파이프라인이 실패한 것은 아닌 경우에만 알림
                if self.failed_pipelines and len(self.failed_pipelines) < len(
                    self.pipelines
                ):
                    print(
                        f"⚠️ Failed pipelines (restart limit reached): {', '.join(self.failed_pipelines)}"
                    )

                # Wait before next check
                self.global_stop_event.wait(30)  # Check every 30 seconds

            except Exception as e:
                print(f"❌ Error in pipeline monitoring: {e}")
                self.global_stop_event.wait(10)  # Wait before retrying

        print("👁️ Pipeline monitoring stopped")

    def get_pipeline_names(self) -> List[str]:
        """등록된 파이프라인 이름 목록 반환"""
        return list(self.pipelines.keys())


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description="MySQL → Kafka → S3 파이프라인 관리 도구",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  %(prog)s start                          # 파이프라인 시작
  %(prog)s stop                           # 파이프라인 중지  
  %(prog)s restart                        # 파이프라인 재시작
  %(prog)s restart-single <pipeline_name> # 개별 파이프라인 재시작
  %(prog)s stop-single <pipeline_name>    # 개별 파이프라인 중지
  %(prog)s status                         # 파이프라인 상태 확인
  %(prog)s logs                           # 최근 로그 보기
  %(prog)s logs --lines 100               # 최근 100줄 로그 보기
        """,
    )

    parser.add_argument(
        "action",
        choices=[
            "start",
            "stop",
            "restart",
            "restart-single",
            "stop-single",
            "status",
            "logs",
        ],
        help="수행할 작업",
    )

    parser.add_argument(
        "pipeline_name",
        nargs="?",
        help="개별 파이프라인 작업시 파이프라인 이름 (restart-single, stop-single용)",
    )

    parser.add_argument(
        "--lines",
        "-n",
        type=int,
        default=50,
        help="로그 보기시 표시할 줄 수 (기본값: 50)",
    )

    args = parser.parse_args()

    manager = PipelineManagerCLI()

    # 액션 실행
    if args.action == "start":
        success = manager.start_pipeline()
        sys.exit(0 if success else 1)

    elif args.action == "stop":
        success = manager.stop_pipeline()
        sys.exit(0 if success else 1)

    elif args.action == "restart":
        success = manager.restart_pipeline()
        sys.exit(0 if success else 1)

    elif args.action == "restart-single":
        if not args.pipeline_name:
            print("❌ Error: Pipeline name is required for restart-single")
            print("Usage: python pipeline_manager.py restart-single <pipeline_name>")
            sys.exit(1)
        success = manager.restart_single_pipeline(args.pipeline_name)
        sys.exit(0 if success else 1)

    elif args.action == "stop-single":
        if not args.pipeline_name:
            print("❌ Error: Pipeline name is required for stop-single")
            print("Usage: python pipeline_manager.py stop-single <pipeline_name>")
            sys.exit(1)
        success = manager.stop_single_pipeline(args.pipeline_name)
        sys.exit(0 if success else 1)

    elif args.action == "status":
        manager.status_pipeline()
        sys.exit(0)

    elif args.action == "logs":
        manager.show_logs(args.lines)
        sys.exit(0)


if __name__ == "__main__":
    main()
