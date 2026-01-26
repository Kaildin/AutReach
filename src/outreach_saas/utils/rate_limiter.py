"""Thread-safe rate limiter with sliding window algorithm.

Provides global rate limiting to prevent API bans and ensure
fair usage across parallel threads.

Usage:
    from rate_limiter import RateLimiter, global_rate_limiter
    
    # As decorator
    @global_rate_limiter
    def make_request(url):
        return requests.get(url)
    
    # As context manager
    with RateLimiter(max_calls=10, period=1.0):
        requests.get(url)
"""
import time
import random
from threading import Lock
from collections import deque
from typing import Callable, Any
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe rate limiter using sliding window algorithm.
    
    Tracks request timestamps and enforces maximum calls per period.
    Safe for use across multiple threads.
    
    Args:
        max_calls: Maximum number of calls allowed
        period: Time period in seconds (default 1.0)
        randomize_delay: Add random jitter to avoid thundering herd (default True)
    
    Example:
        >>> limiter = RateLimiter(max_calls=10, period=1.0)
        >>> 
        >>> @limiter
        >>> def api_call():
        >>>     return requests.get('https://api.example.com')
        >>>
        >>> # Will automatically throttle to 10 calls/second
        >>> api_call()
    """
    
    def __init__(self, max_calls: int, period: float = 1.0, randomize_delay: bool = True):
        self.max_calls = max_calls
        self.period = period
        self.randomize_delay = randomize_delay
        self.calls = deque()
        self.lock = Lock()
        
        logger.debug(f"RateLimiter initialized: {max_calls} calls per {period}s")
    
    def __call__(self, func: Callable) -> Callable:
        """Decorator usage."""
        def wrapper(*args, **kwargs):
            self.wait_if_needed()
            return func(*args, **kwargs)
        return wrapper
    
    def __enter__(self):
        """Context manager entry."""
        self.wait_if_needed()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        pass
    
    def wait_if_needed(self):
        """Wait if rate limit would be exceeded."""
        with self.lock:
            now = time.time()
            
            # Remove old calls outside the sliding window
            while self.calls and self.calls[0] < now - self.period:
                self.calls.popleft()
            
            # If at limit, calculate wait time
            if len(self.calls) >= self.max_calls:
                oldest_call = self.calls[0]
                wait_time = self.period - (now - oldest_call)
                
                if wait_time > 0:
                    # Add random jitter (0-20% of wait time)
                    if self.randomize_delay:
                        jitter = random.uniform(0, wait_time * 0.2)
                        wait_time += jitter
                    
                    logger.debug(f"Rate limit reached, sleeping {wait_time:.3f}s")
                    time.sleep(wait_time)
                    
                    # Clean up again after sleep
                    now = time.time()
                    while self.calls and self.calls[0] < now - self.period:
                        self.calls.popleft()
            
            # Register this call
            self.calls.append(now)
    
    def get_current_rate(self) -> float:
        """Get current request rate (calls/second)."""
        with self.lock:
            now = time.time()
            # Count calls in last period
            recent_calls = sum(1 for t in self.calls if t > now - self.period)
            return recent_calls / self.period if self.period > 0 else 0
    
    def reset(self):
        """Reset the rate limiter (clear all tracked calls)."""
        with self.lock:
            self.calls.clear()
            logger.debug("Rate limiter reset")


# Global rate limiter instances for common use cases
global_rate_limiter = RateLimiter(max_calls=10, period=1.0)  # 10 req/s (safe for Google)
conservative_rate_limiter = RateLimiter(max_calls=5, period=1.0)  # 5 req/s (very safe)
aggressive_rate_limiter = RateLimiter(max_calls=20, period=1.0)  # 20 req/s (risky!)


def rate_limited_sleep(min_delay: float = 0.1, max_delay: float = 0.5):
    """Simple randomized delay for additional protection.
    
    Use this in addition to RateLimiter for extra safety.
    
    Args:
        min_delay: Minimum sleep time in seconds
        max_delay: Maximum sleep time in seconds
    """
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)


# Convenience function for one-off rate limiting
def with_rate_limit(max_calls: int = 10, period: float = 1.0):
    """Decorator factory for custom rate limits.
    
    Example:
        >>> @with_rate_limit(max_calls=5, period=1.0)
        >>> def slow_api_call():
        >>>     return requests.get('https://slow-api.com')
    """
    limiter = RateLimiter(max_calls=max_calls, period=period)
    return limiter


if __name__ == "__main__":
    # Test rate limiter
    import requests
    
    logging.basicConfig(level=logging.DEBUG)
    
    print("Testing RateLimiter...\n")
    
    @global_rate_limiter
    def test_request(i):
        print(f"Request {i} at {time.time():.2f}")
        return i
    
    # Should take ~1 second (10 requests at 10/s)
    start = time.time()
    for i in range(10):
        test_request(i)
    elapsed = time.time() - start
    
    print(f"\n10 requests completed in {elapsed:.2f}s")
    print(f"Expected: ~1.0s, Actual: {elapsed:.2f}s")
    print(f"Current rate: {global_rate_limiter.get_current_rate():.1f} req/s")
