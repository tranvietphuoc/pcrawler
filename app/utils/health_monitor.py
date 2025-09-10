import asyncio
import psutil
import time
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from app.utils.circuit_breaker import circuit_manager
from threading import Lock

logger = logging.getLogger(__name__)

@dataclass
class HealthStatus:
    """Optimized health status data structure"""
    timestamp: float
    worker_id: str
    memory_usage_mb: float
    cpu_percent: float
    active_tasks: int
    browser_count: int
    context_count: int
    circuit_breakers: Dict[str, Any] = field(default_factory=dict)
    is_healthy: bool = True
    issues: list = field(default_factory=list)

class HealthMonitor:
    """
    Optimized health monitoring system for workers and resources
    """
    
    def __init__(self, worker_id: str = None):
        self.worker_id = worker_id or f"worker_{psutil.Process().pid}"
        self._health_history = []
        self._max_history = 50  # Reduced from 100
        self._last_cleanup = time.time()
        self._lock = Lock()
        
        # Health thresholds (optimized)
        self.memory_threshold_mb = 1500  # 1.5GB
        self.cpu_threshold_percent = 80
        self.max_active_tasks = 10
        self.max_browser_count = 5
        self.max_context_count = 8
        
        # Cache for performance
        self._last_health_check = 0.0
        self._health_cache_ttl = 5.0  # Cache for 5 seconds
        self._cached_health = None
        
        # Process cache
        self._process = psutil.Process()
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"HealthMonitor initialized for {self.worker_id}")
    
    async def check_health(self, context_manager=None) -> HealthStatus:
        """Optimized comprehensive health check with caching"""
        current_time = time.time()
        
        # Use cache if still valid
        if (current_time - self._last_health_check < self._health_cache_ttl and 
            self._cached_health is not None):
            return self._cached_health
        
        try:
            # Get system metrics (optimized)
            memory_info = self._process.memory_info()
            memory_usage_mb = memory_info.rss / 1048576  # Faster than /1024/1024
            cpu_percent = self._process.cpu_percent()
            
            # Get browser/context metrics if context manager available (optimized)
            browser_count = 0
            context_count = 0
            if context_manager:
                try:
                    browser_count = len(getattr(context_manager, '_browsers', {}))
                    context_count = sum(getattr(context_manager, '_active_contexts', {}).values())
                except:
                    pass
            
            # Get circuit breaker states (cached)
            circuit_breakers = await circuit_manager.get_all_states()
            
            # Count active tasks (optimized)
            active_tasks = len([task for task in asyncio.all_tasks() if not task.done()])
            
            # Determine health status (optimized)
            issues = []
            is_healthy = True
            
            # Use early returns for better performance
            if memory_usage_mb > self.memory_threshold_mb:
                issues.append(f"High memory usage: {memory_usage_mb:.1f}MB > {self.memory_threshold_mb}MB")
                is_healthy = False
            
            if cpu_percent > self.cpu_threshold_percent:
                issues.append(f"High CPU usage: {cpu_percent:.1f}% > {self.cpu_threshold_percent}%")
                is_healthy = False
            
            if active_tasks > self.max_active_tasks:
                issues.append(f"Too many active tasks: {active_tasks} > {self.max_active_tasks}")
                is_healthy = False
            
            if browser_count > self.max_browser_count:
                issues.append(f"Too many browsers: {browser_count} > {self.max_browser_count}")
                is_healthy = False
            
            if context_count > self.max_context_count:
                issues.append(f"Too many contexts: {context_count} > {self.max_context_count}")
                is_healthy = False
            
            # Check circuit breakers (optimized)
            open_breakers = [name for name, state in circuit_breakers.items() 
                           if state.get('state') == 'OPEN']
            if open_breakers:
                issues.append(f"Open circuit breakers: {open_breakers}")
                is_healthy = False
            
            # Create health status
            health_status = HealthStatus(
                timestamp=current_time,
                worker_id=self.worker_id,
                memory_usage_mb=memory_usage_mb,
                cpu_percent=cpu_percent,
                active_tasks=active_tasks,
                browser_count=browser_count,
                context_count=context_count,
                circuit_breakers=circuit_breakers,
                is_healthy=is_healthy,
                issues=issues
            )
            
            # Cache the result
            self._cached_health = health_status
            self._last_health_check = current_time
            
            # Store in history (optimized)
            with self._lock:
                self._health_history.append(health_status)
                if len(self._health_history) > self._max_history:
                    self._health_history.pop(0)
            
            # Log health status (only when unhealthy or debug mode)
            if not is_healthy:
                logger.warning(f"Health check FAILED: {', '.join(issues)}")
            elif logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Health check OK: {memory_usage_mb:.1f}MB, {cpu_percent:.1f}% CPU, {active_tasks} tasks")
            
            return health_status
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return HealthStatus(
                timestamp=current_time,
                worker_id=self.worker_id,
                memory_usage_mb=0,
                cpu_percent=0,
                active_tasks=0,
                browser_count=0,
                context_count=0,
                circuit_breakers={},
                is_healthy=False,
                issues=[f"Health check error: {e}"]
            )
    
    async def cleanup_if_needed(self, context_manager=None):
        """Perform cleanup if health is poor"""
        health = await self.check_health(context_manager)
        
        if not health.is_healthy:
            logger.warning(f"Performing cleanup due to health issues: {health.issues}")
            
            # Force garbage collection
            import gc
            gc.collect()
            
            # Cleanup contexts if available
            if context_manager and health.context_count > self.max_context_count:
                logger.info("Cleaning up excess contexts")
                # Context cleanup logic would go here
            
            # Reset circuit breakers if too many are open
            open_breakers = [name for name, state in health.circuit_breakers.items() 
                           if state.get('state') == 'OPEN']
            if len(open_breakers) > 2:
                logger.info(f"Resetting circuit breakers: {open_breakers}")
                for breaker_name in open_breakers:
                    circuit_manager.reset_breaker(breaker_name)
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for monitoring"""
        if not self._health_history:
            return {"status": "no_data"}
        
        latest = self._health_history[-1]
        recent = self._health_history[-10:] if len(self._health_history) >= 10 else self._health_history
        
        return {
            "worker_id": self.worker_id,
            "current_status": "healthy" if latest.is_healthy else "unhealthy",
            "current_issues": latest.issues,
            "memory_usage_mb": latest.memory_usage_mb,
            "cpu_percent": latest.cpu_percent,
            "active_tasks": latest.active_tasks,
            "browser_count": latest.browser_count,
            "context_count": latest.context_count,
            "avg_memory_10_checks": sum(h.memory_usage_mb for h in recent) / len(recent),
            "avg_cpu_10_checks": sum(h.cpu_percent for h in recent) / len(recent),
            "unhealthy_checks": sum(1 for h in recent if not h.is_healthy),
            "total_checks": len(self._health_history)
        }

# Global health monitor
health_monitor = HealthMonitor()
