"""
Slack messaging utility for pipeline notifications
"""

from slack_sdk import WebClient
import warnings
from typing import List, Dict, Optional

warnings.filterwarnings("ignore")


class SlackMessenger:
    """Slack 메시지 전송을 위한 유틸리티 클래스 (App 기반)"""

    def __init__(
        self,
        token: Optional[str] = None,
        channel_id: Optional[str] = None,
        webhook_url: Optional[str] = None,  # 기존 호환성 유지
    ):
        """Initialize with Slack token and channel ID"""
        self.token = token
        self.channel_id = channel_id
        self.client = None
        self.last_message_ts = None  # 마지막 메시지의 timestamp 저장 (thread용)

        if token and channel_id:
            self.client = WebClient(token=token)
        else:
            print(
                "⚠️ Slack token or channel_id not configured - messages will not be sent"
            )

    def send_message(
        self,
        text: str = None,
        block: list = None,
        thread_ts: str = None,
        attachments: list = None,
    ) -> dict:
        """기본 메시지 전송 메서드"""
        if not self.client:
            print("⚠️ Slack client not configured - message not sent")
            return {}

        try:
            return self.client.chat_postMessage(
                channel=self.channel_id,
                text=text,
                blocks=block,
                thread_ts=thread_ts,
                attachments=attachments,
            )
        except Exception as e:
            print(f"❌ Failed to send Slack message: {e}")
            return {}

    def send_slack(
        self,
        text: str,
        block_message: Optional[List[Dict]] = None,
        color: str = "#36a64f",
        thread_ts: Optional[str] = None,
        store_timestamp: bool = False,
    ) -> bool:
        """
        Slack으로 메시지 전송

        Args:
            text: 메시지 텍스트
            block_message: Slack block 형식의 메시지 (선택사항)
            color: 메시지 색상 (기본값: 초록색)
            thread_ts: 스레드로 보낼 때 원본 메시지의 timestamp
            store_timestamp: 이 메시지의 timestamp를 저장할지 여부

        Returns:
            bool: 전송 성공 여부
        """
        if not self.client:
            print("⚠️ Slack client not configured - message not sent")
            return False

        try:
            # 메인 메시지 전송 (attachments로 색상 포함)
            response = self.send_message(
                attachments=[{"color": color, "text": text}],
                thread_ts=thread_ts,
            )

            if not response:
                return False

            # timestamp 저장
            if store_timestamp and response.get("ts"):
                self.last_message_ts = response.get("ts")

            # 블록 메시지가 있으면 스레드로 전송
            if block_message:
                thread_ts_to_use = thread_ts or response.get("ts")
                if thread_ts_to_use:
                    block_response = self.send_message(
                        block=block_message, thread_ts=thread_ts_to_use
                    )
                    return bool(block_response)
                else:
                    print(
                        "Warning: Could not get thread_ts from initial Slack message."
                    )
                    return False

            return True

        except Exception as e:
            print(f"❌ Failed to send Slack message: {e}")
            return False

    def send_slack_with_thread(
        self,
        initial_text: str,
        follow_up_text: str = None,
        follow_up_blocks: Optional[List[Dict]] = None,
        color: str = "#36a64f",
    ) -> bool:
        """
        초기 메시지를 보내고, 후속 메시지를 스레드로 전송

        Args:
            initial_text: 초기 메시지 텍스트
            follow_up_text: 스레드로 보낼 후속 메시지 텍스트
            follow_up_blocks: 스레드로 보낼 후속 메시지 블록
            color: 메시지 색상

        Returns:
            bool: 전송 성공 여부
        """
        # 초기 메시지 전송 (timestamp 저장)
        success = self.send_slack(initial_text, color=color, store_timestamp=True)

        if not success:
            return False

        # 후속 메시지가 있으면 스레드로 전송
        if follow_up_text or follow_up_blocks:
            return self.send_slack(
                text=follow_up_text or "Status Details",
                block_message=follow_up_blocks,
                color=color,
                thread_ts=self.last_message_ts,
            )

        return True

    def send_error_alert(
        self,
        pipeline_name: str,
        error_message: str,
        detailed_error: Optional[str] = None,
    ) -> bool:
        """
        에러 알림 전송

        Args:
            pipeline_name: 파이프라인 이름
            error_message: 간단한 에러 메시지
            detailed_error: 상세 에러 정보 (선택사항)

        Returns:
            bool: 전송 성공 여부
        """
        text = f"🚨 *{pipeline_name} Pipeline Error*\n{error_message}"

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

        return self.send_slack(text, block_message, color="#ff0000")

    def send_warning_alert(self, pipeline_name: str, warning_message: str) -> bool:
        """
        경고 알림 전송

        Args:
            pipeline_name: 파이프라인 이름
            warning_message: 경고 메시지

        Returns:
            bool: 전송 성공 여부
        """
        text = f"⚠️ *{pipeline_name} Pipeline Warning*\n{warning_message}"
        return self.send_slack(text, color="#ffa500")

    def send_info_alert(self, pipeline_name: str, info_message: str) -> bool:
        """
        정보 알림 전송

        Args:
            pipeline_name: 파이프라인 이름
            info_message: 정보 메시지

        Returns:
            bool: 전송 성공 여부
        """
        text = f"ℹ️ *{pipeline_name} Pipeline Info*\n{info_message}"
        return self.send_slack(text, color="#36a64f")

    def send_circuit_breaker_alert(
        self,
        pipeline_name: str,
        state: str,
        consecutive_failures: int,
        recovery_time: Optional[float] = None,
    ) -> bool:
        """
        Circuit Breaker 상태 변경 알림

        Args:
            pipeline_name: 파이프라인 이름
            state: Circuit Breaker 상태 (OPEN, CLOSED, HALF_OPEN)
            consecutive_failures: 연속 실패 횟수
            recovery_time: 복구까지 남은 시간 (OPEN 상태일 때)

        Returns:
            bool: 전송 성공 여부
        """
        if state == "OPEN":
            emoji = "🔴"
            color = "#ff0000"
            message = (
                f"Circuit Breaker OPEN - {consecutive_failures} consecutive failures"
            )
            if recovery_time:
                message += f"\nRecovery attempt in {recovery_time:.1f}s"
        elif state == "HALF_OPEN":
            emoji = "🟡"
            color = "#ffa500"
            message = f"Circuit Breaker HALF_OPEN - attempting recovery"
        else:  # CLOSED
            emoji = "🟢"
            color = "#36a64f"
            message = f"Circuit Breaker CLOSED - system recovered"

        text = f"{emoji} *{pipeline_name} Circuit Breaker*\n{message}"
        return self.send_slack(text, color=color)
