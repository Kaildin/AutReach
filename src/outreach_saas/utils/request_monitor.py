"""Real-time monitoring for HTTP requests and API calls.

Tracks success/failure rates, detects anomalies, and provides
live statistics for debugging and performance monitoring.

Usage:
    from request_monitor import RequestMonitor, global_monitor
    
    # Record requests
    global_monitor.record_request(success=True, service='google')
    
    # Get stats
    stats = global_monitor.get_stats()
    print(stats)
"""
import time
import logging
from threading import Lock
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class RequestStats:
    """Statistics for a group of requests."""
    total: int = 0
    success: int = 0
    failed: int = 0
    rate_limited: int = 0
    avg_response_time: float = 0.0
    response_times: deque = field(default_factory=lambda: deque(maxlen=100))
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate (0.0 to 1.0)."""
        return self.failed / self.total if self.total > 0 else 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate (0.0 to 1.0)."""
        return self.success / self.total if self.total > 0 else 0.0


class RequestMonitor:
    """Thread-safe request monitoring with anomaly detection.
    
    Tracks requests across multiple services/endpoints and provides
    real-time statistics and alerts.
    
    Features:
    - Success/failure tracking
    - Response time monitoring
    - Rate limit detection
    - Anomaly alerts (high failure rate)
    - Per-service statistics
    
    Example:
        >>> monitor = RequestMonitor(alert_threshold=0.15)
        >>> 
        >>> # Record successful request
        >>> monitor.record_request(
        >>>     success=True, 
        >>>     service='google_places',
        >>>     response_time=0.234
        >>> )
        >>> 
        >>> # Check if we should slow down
        >>> if monitor.should_pause():
        >>>     time.sleep(5)
    """
    
    def __init__(self, alert_threshold: float = 0.10, window_size: int = 100):
        """
        Args:
            alert_threshold: Failure rate threshold for alerts (default 10%)
            window_size: Number of recent requests to track for rolling stats
        """
        self.alert_threshold = alert_threshold
        self.window_size = window_size
        
        self.global_stats = RequestStats()
        self.service_stats: Dict[str, RequestStats] = defaultdict(RequestStats)
        
        self.start_time = time.time()
        self.last_alert_time = 0
        self.alert_cooldown = 60  # Don't spam alerts (1 per minute)
        
        self.lock = Lock()
        
        logger.info(f"RequestMonitor initialized (alert_threshold={alert_threshold:.0%})")
    
    def record_request(
        self, 
        success: bool, 
        service: str = 'default',
        response_time: Optional[float] = None,
        status_code: Optional[int] = None
    ):
        """Record a request and update statistics.
        
        Args:
            success: Whether request succeeded
            service: Service/endpoint name for grouping
            response_time: Response time in seconds (optional)
            status_code: HTTP status code (optional)
        """
        with self.lock:
            # Update global stats
            self.global_stats.total += 1
            if success:
                self.global_stats.success += 1
            else:
                self.global_stats.failed += 1
            
            # Track rate limiting
            if status_code == 429:
                self.global_stats.rate_limited += 1
            
            # Track response time
            if response_time is not None:
                self.global_stats.response_times.append(response_time)
                if self.global_stats.response_times:
                    self.global_stats.avg_response_time = sum(self.global_stats.response_times) / len(self.global_stats.response_times)
            
            # Update service-specific stats
            svc_stats = self.service_stats[service]
            svc_stats.total += 1
            if success:
                svc_stats.success += 1
            else:
                svc_stats.failed += 1
            
            if status_code == 429:
                svc_stats.rate_limited += 1
            
            if response_time is not None:
                svc_stats.response_times.append(response_time)
                if svc_stats.response_times:
                    svc_stats.avg_response_time = sum(svc_stats.response_times) / len(svc_stats.response_times)
            
            # Check for anomalies
            self._check_anomalies()
    
    def _check_anomalies(self):
        """Check for high failure rates and alert."""
        failure_rate = self.global_stats.failure_rate
        
        # Only alert if we have enough samples
        if self.global_stats.total < 10:
            return
        
        # Check if failure rate exceeds threshold
        if failure_rate > self.alert_threshold:
            now = time.time()
            # Cooldown to avoid spam
            if now - self.last_alert_time > self.alert_cooldown:
                logger.warning(
                    f"⚠️  HIGH FAILURE RATE DETECTED: {failure_rate:.1%} "
                    f"({self.global_stats.failed}/{self.global_stats.total} requests failed)"
                )
                logger.warning("Consider: reducing parallel workers, adding delays, or checking IP ban")
                self.last_alert_time = now
    
    def should_pause(self) -> bool:
        """Check if we should pause due to high failure rate.
        
        Returns:
            True if failure rate is critically high
        """
        with self.lock:
            # Pause if failure rate > 20% (critical)
            return self.global_stats.failure_rate > 0.20
    
    def get_stats(self, service: Optional[str] = None) -> Dict:
        """Get current statistics.
        
        Args:
            service: Get stats for specific service, or None for global
        
        Returns:
            Dictionary with statistics
        """
        with self.lock:
            if service:
                stats = self.service_stats.get(service, RequestStats())
            else:
                stats = self.global_stats
            
            elapsed = time.time() - self.start_time
            
            return {
                'total': stats.total,
                'success': stats.success,
                'failed': stats.failed,
                'rate_limited': stats.rate_limited,
                'success_rate': stats.success_rate,
                'failure_rate': stats.failure_rate,
                'avg_response_time': stats.avg_response_time,
                'requests_per_second': stats.total / elapsed if elapsed > 0 else 0,
                'elapsed_time': elapsed
            }
    
    def get_all_services_stats(self) -> Dict[str, Dict]:
        """Get statistics for all tracked services."""
        with self.lock:
            return {
                service: self.get_stats(service)
                for service in self.service_stats.keys()
            }
    
    def print_summary(self):
        """Print a formatted summary of statistics."""
        stats = self.get_stats()
        
        print("\n" + "="*60)
        print("REQUEST MONITORING SUMMARY")
        print("="*60)
        print(f"Total Requests:      {stats['total']:,}")
        print(f"Success:             {stats['success']:,} ({stats['success_rate']:.1%})")
        print(f"Failed:              {stats['failed']:,} ({stats['failure_rate']:.1%})")
        print(f"Rate Limited:        {stats['rate_limited']:,}")
        print(f"Avg Response Time:   {stats['avg_response_time']:.3f}s")
        print(f"Request Rate:        {stats['requests_per_second']:.2f} req/s")
        print(f"Elapsed Time:        {stats['elapsed_time']:.1f}s")
        
        # Per-service breakdown
        if len(self.service_stats) > 1:
            print("\nPer-Service Breakdown:")
            print("-" * 60)
            for service, svc_stats in self.get_all_services_stats().items():
                print(f"\n{service}:")
                print(f"  Total: {svc_stats['total']:,} | "
                      f"Success: {svc_stats['success_rate']:.1%} | "
                      f"Failed: {svc_stats['failure_rate']:.1%}")
        
        print("="*60 + "\n")
    
    def reset(self):
        """Reset all statistics."""
        with self.lock:
            self.global_stats = RequestStats()
            self.service_stats.clear()
            self.start_time = time.time()
            logger.info("RequestMonitor reset")


# Global monitor instance
global_monitor = RequestMonitor(alert_threshold=0.10)


if __name__ == "__main__":
    # Test monitor
    logging.basicConfig(level=logging.INFO)
    
    print("Testing RequestMonitor...\n")
    
    monitor = RequestMonitor(alert_threshold=0.15)
    
    # Simulate requests
    for i in range(100):
        success = i % 10 != 0  # 10% failure rate
        monitor.record_request(
            success=success,
            service='test_api',
            response_time=0.1 + (i % 5) * 0.05
        )
    
    # Print summary
    monitor.print_summary()
    
    # Test pause detection
    print(f"Should pause: {monitor.should_pause()}")
