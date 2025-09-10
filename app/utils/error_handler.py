"""
Optimized error handling utilities for better performance
"""

import logging
import time
from typing import Dict, Any, Optional, Callable
from functools import wraps
import asyncio

logger = logging.getLogger(__name__)

class OptimizedErrorHandler:
    """
    Optimized error handler with caching and performance improvements
    """
    
    def __init__(self):
        self._error_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = 60.0  # Cache errors for 60 seconds
        self._max_cache_size = 1000
        
    def is_critical_error(self, error: Exception) -> bool:
        """Fast check if error is critical (cached)"""
        error_type = type(error).__name__
        error_msg = str(error)
        cache_key = f"{error_type}:{hash(error_msg) % 1000}"  # Hash for performance
        
        # Check cache first
        if cache_key in self._error_cache:
            cached = self._error_cache[cache_key]
            if time.time() - cached['timestamp'] < self._cache_ttl:
                return cached['is_critical']
        
        # Determine if critical
        critical_keywords = [
            "Target page, context or browser has been closed",
            "TargetClosedError", 
            "Browser.new_context",
            "BrowserType.launch",
            "Protocol error",
            "Connection lost",
            "Navigation timeout",
            "TimeoutError"
        ]
        
        is_critical = any(keyword in error_msg for keyword in critical_keywords)
        
        # Cache result
        self._error_cache[cache_key] = {
            'is_critical': is_critical,
            'timestamp': time.time()
        }
        
        # Cleanup cache if too large
        if len(self._error_cache) > self._max_cache_size:
            self._cleanup_cache()
        
        return is_critical
    
    def _cleanup_cache(self):
        """Cleanup old cache entries"""
        current_time = time.time()
        expired_keys = [
            key for key, value in self._error_cache.items()
            if current_time - value['timestamp'] > self._cache_ttl
        ]
        for key in expired_keys:
            del self._error_cache[key]
    
    def get_error_category(self, error: Exception) -> str:
        """Get error category for better handling"""
        error_type = type(error).__name__
        error_msg = str(error)
        
        if "TargetClosedError" in error_type or "Target page" in error_msg:
            return "browser_closed"
        elif "TimeoutError" in error_type or "timeout" in error_msg.lower():
            return "timeout"
        elif "Connection" in error_msg or "network" in error_msg.lower():
            return "network"
        elif "Protocol error" in error_msg:
            return "protocol"
        else:
            return "unknown"

# Global error handler instance
error_handler = OptimizedErrorHandler()

def optimized_retry(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Optimized retry decorator with exponential backoff
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        break
                    
                    # Check if error is critical
                    if error_handler.is_critical_error(e):
                        # For critical errors, wait longer
                        wait_time = delay * (backoff ** attempt) * 2
                    else:
                        # For non-critical errors, shorter wait
                        wait_time = delay * (backoff ** attempt)
                    
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Retry {attempt + 1}/{max_retries} after {wait_time:.1f}s: {e}")
                    
                    await asyncio.sleep(wait_time)
            
            # All retries failed
            raise last_exception
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        break
                    
                    # Check if error is critical
                    if error_handler.is_critical_error(e):
                        wait_time = delay * (backoff ** attempt) * 2
                    else:
                        wait_time = delay * (backoff ** attempt)
                    
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Retry {attempt + 1}/{max_retries} after {wait_time:.1f}s: {e}")
                    
                    time.sleep(wait_time)
            
            raise last_exception
        
        # Return appropriate wrapper
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

def fast_error_check(error: Exception) -> Dict[str, Any]:
    """
    Fast error analysis with minimal overhead
    """
    error_type = type(error).__name__
    error_msg = str(error)
    
    return {
        'type': error_type,
        'message': error_msg,
        'is_critical': error_handler.is_critical_error(error),
        'category': error_handler.get_error_category(error),
        'timestamp': time.time()
    }
