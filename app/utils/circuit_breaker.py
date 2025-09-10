import asyncio
import time
import logging
from enum import IntEnum
from typing import Callable, Any, Optional
from threading import Lock
import weakref

logger = logging.getLogger(__name__)

class CircuitState(IntEnum):
    CLOSED = 0      # Normal operation
    OPEN = 1        # Circuit is open, failing fast
    HALF_OPEN = 2   # Testing if service is back

class CircuitBreaker:
    """
    Optimized circuit breaker pattern implementation for preventing cascading failures
    """
    
    def __init__(
        self, 
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception,
        name: str = "default"
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name
        
        self.failure_count = 0
        self.last_failure_time = 0.0  # Use float for faster comparison
        self.state = CircuitState.CLOSED
        self._lock = Lock()  # Use threading.Lock for better performance
        
        # Cache for faster state checks
        self._state_cache = {
            "name": name,
            "state": "CLOSED",
            "failure_count": 0,
            "failure_threshold": failure_threshold,
            "last_failure_time": None,
            "recovery_timeout": recovery_timeout
        }
        
        # Only log in debug mode to reduce overhead
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"CircuitBreaker '{name}' initialized: threshold={failure_threshold}, timeout={recovery_timeout}s")
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Optimized execute function with circuit breaker protection
        """
        # Fast path: check state without lock first
        if self.state == CircuitState.OPEN:
            if not self._should_attempt_reset():
                raise Exception(f"CircuitBreaker '{self.name}' is OPEN - failing fast")
        
        # Use threading lock for better performance
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self._update_cache()
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"CircuitBreaker '{self.name}' transitioning to HALF_OPEN")
                else:
                    raise Exception(f"CircuitBreaker '{self.name}' is OPEN - failing fast")
            
            try:
                # Execute the function
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                # Success - reset failure count
                if self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.CLOSED
                    self._update_cache()
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"CircuitBreaker '{self.name}' transitioning to CLOSED")
                
                self.failure_count = 0
                self._update_cache()
                return result
                
            except self.expected_exception as e:
                self._record_failure()
                raise e
    
    def _record_failure(self):
        """Optimized record a failure and update circuit state"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        # Only log warnings/errors, not every failure
        if self.failure_count == self.failure_threshold:
            logger.warning(f"CircuitBreaker '{self.name}' failure count: {self.failure_count}/{self.failure_threshold}")
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self._update_cache()
            logger.error(f"CircuitBreaker '{self.name}' is now OPEN - failing fast for {self.recovery_timeout}s")
    
    def _should_attempt_reset(self) -> bool:
        """Optimized check if enough time has passed to attempt reset"""
        if self.last_failure_time == 0.0:
            return True
        
        return time.time() - self.last_failure_time >= self.recovery_timeout
    
    def _update_cache(self):
        """Update state cache for faster access"""
        self._state_cache.update({
            "state": self.state.name,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time if self.last_failure_time > 0 else None
        })
    
    def get_state(self) -> dict:
        """Get current circuit breaker state (cached for performance)"""
        return self._state_cache.copy()

class CircuitBreakerManager:
    """
    Optimized manager for multiple circuit breakers
    """
    
    def __init__(self):
        self._breakers: dict = {}
        self._lock = Lock()  # Use threading.Lock for better performance
        self._states_cache = {}  # Cache for faster state access
        self._cache_ttl = 1.0  # Cache TTL in seconds
        self._last_cache_update = 0.0
    
    def get_breaker(self, name: str, **kwargs) -> CircuitBreaker:
        """Get or create a circuit breaker (optimized)"""
        if name not in self._breakers:
            with self._lock:
                # Double-check pattern
                if name not in self._breakers:
                    self._breakers[name] = CircuitBreaker(name=name, **kwargs)
        return self._breakers[name]
    
    async def get_all_states(self) -> dict:
        """Get states of all circuit breakers (cached)"""
        current_time = time.time()
        
        # Use cache if still valid
        if current_time - self._last_cache_update < self._cache_ttl and self._states_cache:
            return self._states_cache.copy()
        
        # Update cache
        with self._lock:
            self._states_cache = {name: breaker.get_state() for name, breaker in self._breakers.items()}
            self._last_cache_update = current_time
        
        return self._states_cache.copy()
    
    def reset_breaker(self, name: str):
        """Reset a specific circuit breaker (optimized)"""
        if name in self._breakers:
            breaker = self._breakers[name]
            breaker.state = CircuitState.CLOSED
            breaker.failure_count = 0
            breaker.last_failure_time = 0.0
            breaker._update_cache()
            
            # Invalidate cache
            self._last_cache_update = 0.0
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"CircuitBreaker '{name}' manually reset")

# Global circuit breaker manager
circuit_manager = CircuitBreakerManager()
