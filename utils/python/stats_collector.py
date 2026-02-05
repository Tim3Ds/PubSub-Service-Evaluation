#!/usr/bin/env python3
"""
Stats Collector - Performance metrics collection for all services.
Provides compatibility with legacy tests using MessageStats class.
"""
import time
import json
from typing import Dict, Any, List

# Try to import MessagingStats from messaging, but fallback if not found
try:
    from messaging import MessagingStats as BaseStats, get_current_time_ms
except ImportError:
    # Minimal implementation if messaging.py is not in path
    def get_current_time_ms() -> float:
        return time.time() * 1000

    class BaseStats:
        def __init__(self):
            self.sent_count = 0
            self.received_count = 0
            self.failed_count = 0
            self.message_timings: List[float] = []
            self.start_time = 0
            self.end_time = 0
            self.metadata: Dict[str, Any] = {}

        def record_send(self, success: bool, timing_ms: float = 0.0):
            self.sent_count += 1
            if success:
                self.received_count += 1
                if timing_ms > 0:
                    self.message_timings.append(timing_ms)
            else:
                self.failed_count += 1

        def set_duration(self, start_ms: float, end_ms: float):
            self.start_time = start_ms
            self.end_time = end_ms

        def get_duration_ms(self) -> float:
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return 0.0

        def get_stats(self) -> Dict[str, Any]:
            duration = self.get_duration_ms()
            stats = {
                "total_sent": self.sent_count,
                "total_received": self.received_count,
                "total_failed": self.failed_count,
                "duration_ms": duration
            }
            stats.update(self.metadata)
            if self.message_timings:
                stats["message_timing_stats"] = {
                    "min_ms": min(self.message_timings),
                    "max_ms": max(self.message_timings),
                    "mean_ms": sum(self.message_timings) / len(self.message_timings),
                    "count": len(self.message_timings)
                }
            return stats

class MessageStats(BaseStats):
    """
    Compatibility wrapper for MessageStats used in many test files.
    """
    def __init__(self):
        super().__init__()
        # Ensure we have metadata dict even if not in BaseStats
        if not hasattr(self, 'metadata'):
            self.metadata = {}

    def record_message(self, success: bool, timing_ms: float = 0.0):
        """Map record_message (C++ style) to record_send (Unified style)."""
        self.record_send(success, timing_ms)
        
    def set_metadata(self, metadata: Dict[str, Any]):
        """Set metadata for reporting."""
        self.metadata.update(metadata)

def get_current_time_ms_static() -> float:
    """Static helper for time."""
    return time.time() * 1000
