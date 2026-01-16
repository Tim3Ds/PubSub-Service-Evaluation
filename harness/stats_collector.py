#!/usr/bin/env python3
"""
Statistics Collector for normalized test reporting.
Provides utilities for tracking message timings and computing min/median/max statistics.
"""
import statistics
import time
from typing import List, Dict, Any


class MessageStats:
    """Tracks timing statistics for individual messages."""
    
    def __init__(self):
        self.message_timings: List[float] = []  # milliseconds
        self.sent_count = 0
        self.received_count = 0
        self.processed_count = 0
        self.failed_count = 0
        self.start_time = None
        self.end_time = None
    
    def record_message(self, success: bool, timing_ms: float = 0):
        """Record a single message result."""
        self.sent_count += 1
        if success:
            self.received_count += 1
            self.processed_count += 1
            if timing_ms > 0:
                self.message_timings.append(timing_ms)
        else:
            self.failed_count += 1
    
    def set_duration(self, start_ms: float, end_ms: float):
        """Set the overall test duration."""
        self.start_time = start_ms
        self.end_time = end_ms
    
    def get_duration_ms(self) -> float:
        """Get total duration in milliseconds."""
        if self.start_time is not None and self.end_time is not None:
            return self.end_time - self.start_time
        return 0.0
    
    def get_stats(self) -> Dict[str, Any]:
        """Generate statistics dictionary."""
        duration = self.get_duration_ms()
        stats = {
            'total_sent': self.sent_count,
            'total_received': self.received_count,
            'total_processed': self.processed_count,
            'total_failed': self.failed_count,
            'duration_ms': duration,
            'processed_per_ms': self.processed_count / duration if duration > 0 else 0,
            'failed_per_ms': self.failed_count / duration if duration > 0 else 0
        }
        
        # Add message timing statistics if we have data
        if self.message_timings:
            stats['message_timing_stats'] = {
                'min_ms': min(self.message_timings),
                'max_ms': max(self.message_timings),
                'mean_ms': statistics.mean(self.message_timings),
                'median_ms': statistics.median(self.message_timings),
                'count': len(self.message_timings)
            }
            if len(self.message_timings) > 1:
                stats['message_timing_stats']['stdev_ms'] = statistics.stdev(self.message_timings)
        
        return stats


def get_current_time_ms() -> float:
    """Get current time in milliseconds."""
    return time.time() * 1000
