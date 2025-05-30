"""
Historify - Stock Historical Data Management App
Rate Limiter Utility
"""
import time
import threading
import logging
from functools import wraps

class RateLimiter:
    """
    Rate limiter to control API request frequency
    
    Ensures we don't exceed broker's rate limits (e.g., 10 symbols per second)
    """
    def __init__(self, max_calls, period=1.0):
        """
        Initialize rate limiter
        
        Args:
            max_calls: Maximum number of calls allowed in the period
            period: Time period in seconds (default: 1 second)
        """
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()
        
    def __call__(self, func):
        """
        Decorator to rate limit function calls
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self.lock:
                # Clean up old calls
                now = time.time()
                self.calls = [call_time for call_time in self.calls if call_time > now - self.period]
                
                # Check if we've reached the limit
                if len(self.calls) >= self.max_calls:
                    # Wait until we can make another call
                    sleep_time = self.calls[0] + self.period - now
                    if sleep_time > 0:
                        logging.info(f"Rate limit reached. Waiting {sleep_time:.2f} seconds before next call.")
                        time.sleep(sleep_time)
                        # Clean up again after waiting
                        now = time.time()
                        self.calls = [call_time for call_time in self.calls if call_time > now - self.period]
                
                # Add current call time
                self.calls.append(now)
                
            # Execute the function
            return func(*args, **kwargs)
        
        return wrapper

def batch_process(items, batch_size, process_func, *args, **kwargs):
    """
    Process items in batches to respect rate limits
    
    Args:
        items: List of items to process
        batch_size: Number of items to process in each batch
        process_func: Function to process each batch
        *args, **kwargs: Additional arguments to pass to process_func
        
    Returns:
        List of results from processing all batches
    """
    results = []
    
    # Process in batches
    for i in range(0, len(items), batch_size):
        batch = items[i:i+batch_size]
        batch_results = process_func(batch, *args, **kwargs)
        results.extend(batch_results)
        
        # If this isn't the last batch, wait to respect rate limits
        if i + batch_size < len(items):
            logging.info(f"Processed batch of {len(batch)} items. Waiting before next batch.")
            time.sleep(1.0)  # Wait 1 second between batches
    
    return results

# Create rate limiters with different configurations
# For broker API calls (10 symbols per second)
broker_rate_limiter = RateLimiter(max_calls=10, period=1.0)
