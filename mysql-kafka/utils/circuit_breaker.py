"""
Circuit Breaker Pattern Implementation for Pipeline Error Handling
"""

import time


# Circuit breaker configuration
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5  # 연속 실패 임계값
CIRCUIT_BREAKER_RECOVERY_TIMEOUT = 60  # 복구 대기 시간 (초)
CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS = 3  # Half-open 상태에서 최대 시도 횟수


class CircuitBreakerState:
    """Circuit breaker states"""

    CLOSED = "CLOSED"  # 정상 동작
    OPEN = "OPEN"  # 장애 상태 - 호출 차단
    HALF_OPEN = "HALF_OPEN"  # 복구 시도 상태


class CircuitBreaker:
    """Circuit Breaker implementation for handling failures gracefully"""

    def __init__(
        self,
        failure_threshold=CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        recovery_timeout=CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
        half_open_max_calls=CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        # State tracking
        self.state = CircuitBreakerState.CLOSED
        self.consecutive_failures = 0
        self.last_failure_time = 0
        self.half_open_calls = 0

    def can_execute(self) -> bool:
        """Check if circuit breaker allows the operation."""
        current_time = time.time()

        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            # Check if enough time has passed to try recovery
            if current_time - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls = 0
                return True
            return False
        elif self.state == CircuitBreakerState.HALF_OPEN:
            return self.half_open_calls < self.half_open_max_calls

        return False

    def record_success(self) -> None:
        """Record successful operation and reset circuit breaker if needed."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.CLOSED

        self.consecutive_failures = 0
        self.half_open_calls = 0

    def record_failure(self) -> None:
        """Record failed operation and update circuit breaker state."""
        self.consecutive_failures += 1
        self.last_failure_time = time.time()

        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_calls += 1
            if self.half_open_calls >= self.half_open_max_calls:
                self.state = CircuitBreakerState.OPEN
        elif self.consecutive_failures >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN

    def get_status(self) -> dict:
        """Get circuit breaker status information."""
        return {
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "half_open_calls": self.half_open_calls,
            "last_failure_time": self.last_failure_time,
            "time_until_recovery": (
                max(0, self.recovery_timeout - (time.time() - self.last_failure_time))
                if self.state == CircuitBreakerState.OPEN
                else 0
            ),
        }
