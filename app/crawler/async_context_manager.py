import asyncio
import logging
import psutil
import gc
import os
import time
import weakref
from contextlib import asynccontextmanager
from typing import Dict, Optional, Any
from playwright.async_api import async_playwright
from crawl4ai import AsyncWebCrawler

logger = logging.getLogger(__name__)

class AsyncBrowserContextManager:
    """
    Enhanced async context manager for browser instances with process isolation,
    memory monitoring, and enhanced error recovery.
    """
    
    def __init__(self):
        # Use weak references for better memory management
        self._browsers: Dict[str, weakref.ref] = {}
        self._active_contexts: Dict[str, int] = {}
        self._request_counts: Dict[str, int] = {}
        self._last_restart: Dict[str, float] = {}
        self._memory_usage: Dict[str, float] = {}
        
        # Auto-generate worker_id from container name or environment
        container_name = os.getenv('HOSTNAME', 'default')
        process_id = os.getpid()
        thread_id = id(asyncio.current_task()) if asyncio.current_task() else 0
        # Add unique timestamp and random component for better isolation
        import uuid
        unique_id = str(uuid.uuid4())[:8]
        self._worker_id = f"worker_{container_name}_{process_id}_{thread_id}_{unique_id}"
        self._process_id = process_id
        
        # Create worker-specific lock (not singleton)
        self._lock = asyncio.Lock()
        
        # Enhanced resource limits with process isolation - SCALING OPTIMIZED
        self._max_contexts_per_worker = 3  # Giảm xuống 3 cho scaling stability
        self._browser_restart_threshold = 150  # Giảm xuống 150 cho scaling
        self._context_lifetime = 480  # Giảm xuống 480s (8 phút) cho scaling
        self._memory_threshold_mb = 500  # Giảm xuống 500MB per browser cho scaling
        self._max_memory_per_worker_mb = 1000  # Giảm xuống 1000MB per worker cho scaling
        
        # Browser persistence settings
        self._browser_persistence_enabled = True  # Enable browser persistence
        self._browser_keepalive_interval = 30  # Keep-alive check every 30 seconds
        self._browser_keepalive_timeout = 10  # Keep-alive timeout 10 seconds
        self._browser_auto_reconnect = True  # Auto-reconnect on failure
        
        # Process isolation settings
        self._worker_browser_pool = {}  # Separate browser pool per worker
        self._worker_memory_tracker = {}  # Memory tracking per worker
        self._restart_suspended: Dict[str, bool] = {}  # Suspend restarts per worker
        
        # Browser persistence tracking
        self._browser_last_activity = {}  # Track last activity time
        self._browser_keepalive_tasks = {}  # Keep-alive background tasks
        self._browser_connection_status = {}  # Track connection status
        
        # Task isolation tracking
        self._active_tasks = set()  # Track active tasks
        self._task_browser_mapping = {}  # Map tasks to browsers
        
        # Cache for performance
        self._process = psutil.Process()
        self._last_memory_check = 0.0
        self._memory_cache_ttl = 2.0  # Cache memory check for 2 seconds
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"AsyncBrowserContextManager initialized for worker {self._worker_id} (PID: {self._process_id})")
    
    async def _get_worker_memory_usage(self) -> float:
        """Get current memory usage for this worker process (optimized with caching)"""
        current_time = time.time()
        
        # Use cache if still valid
        if current_time - self._last_memory_check < self._memory_cache_ttl:
            return getattr(self, '_cached_memory_usage', 0.0)
        
        try:
            memory_mb = self._process.memory_info().rss / 1048576  # Faster than /1024/1024
            
            # Cache the result
            self._cached_memory_usage = memory_mb
            self._last_memory_check = current_time
            
            return memory_mb
        except Exception as e:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Failed to get memory usage: {e}")
            return 0.0
    
    async def _check_memory_pressure(self) -> bool:
        """Check if worker is under memory pressure"""
        try:
            memory_mb = await self._get_worker_memory_usage()
            self._worker_memory_tracker[self._worker_id] = memory_mb
            
            if memory_mb > self._max_memory_per_worker_mb:
                logger.warning(f"Worker {self._worker_id} memory pressure: {memory_mb:.1f}MB > {self._max_memory_per_worker_mb}MB")
                return True
            
            # Check system memory
            system_memory = psutil.virtual_memory()
            if system_memory.percent > 85:
                logger.warning(f"System memory pressure: {system_memory.percent}%")
                return True
                
            return False
        except Exception as e:
            logger.warning(f"Memory check failed: {e}")
            return False
    
    async def _force_garbage_collection(self):
        """Force garbage collection to free memory"""
        try:
            gc.collect()
            await asyncio.sleep(0.1)  # Allow GC to complete
            logger.debug(f"Garbage collection completed for worker {self._worker_id}")
        except Exception as e:
            logger.warning(f"Garbage collection failed: {e}")
    
    async def _browser_keepalive(self, worker_key: str):
        """Keep browser alive with periodic health checks"""
        while worker_key in self._worker_browser_pool:
            try:
                await asyncio.sleep(self._browser_keepalive_interval)
                
                if worker_key not in self._worker_browser_pool:
                    break
                
                browser = self._worker_browser_pool[worker_key]
                
                # Quick health check
                try:
                    if hasattr(browser, "is_connected"):
                        if not browser.is_connected():
                            logger.warning(f"Browser {worker_key} disconnected during keepalive, reconnecting...")
                            await self._restart_browser(worker_key)
                            continue
                    
                    # Update last activity
                    self._browser_last_activity[worker_key] = time.time()
                    self._browser_connection_status[worker_key] = "connected"
                    
                except Exception as e:
                    logger.warning(f"Browser {worker_key} keepalive check failed: {e}")
                    if self._browser_auto_reconnect:
                        await self._restart_browser(worker_key)
                    else:
                        self._browser_connection_status[worker_key] = "disconnected"
                        
            except Exception as e:
                logger.error(f"Browser keepalive error for {worker_key}: {e}")
                await asyncio.sleep(5)  # Wait before retry
    
    async def _get_or_create_browser(self, crawler_id: str, task_id: str = None):
        """Get or create browser with process isolation and memory monitoring"""
        # Include task_id for better isolation
        if task_id:
            worker_key = f"{self._worker_id}_{crawler_id}_{task_id}"
        else:
            worker_key = f"{self._worker_id}_{crawler_id}"
        
        # Check if we need to restart due to memory pressure (skip if suspended)
        if not self._restart_suspended.get(self._worker_id, False) and await self._check_memory_pressure():
            logger.warning(f"Memory pressure detected, restarting all browsers for worker {self._worker_id}")
            await self._restart_all_worker_browsers()
            await self._force_garbage_collection()
        
        # Check if browser exists and is healthy
        if worker_key in self._worker_browser_pool:
            browser = self._worker_browser_pool[worker_key]
            try:
                # Enhanced health check with timeout
                if hasattr(browser, "is_connected"):
                    if not browser.is_connected():
                        raise Exception("Browser disconnected")
                else:
                    # Fallback: try to access version safely with timeout
                    ver = None
                    try:
                        ver = await asyncio.wait_for(browser.version(), timeout=5.0)  # 5s timeout
                    except (TypeError, asyncio.TimeoutError):
                        try:
                            ver = browser.version  # property access
                        except Exception:
                            pass
                    if not ver:
                        raise Exception("Browser health check failed: version unavailable")
                
                # Check browser memory usage
                if not self._restart_suspended.get(self._worker_id, False) and worker_key in self._memory_usage:
                    memory_mb = self._memory_usage[worker_key]
                    if memory_mb > self._memory_threshold_mb:
                        logger.warning(f"Browser {worker_key} memory usage too high: {memory_mb:.1f}MB")
                        await self._restart_browser(worker_key)
                        browser = await self._create_new_browser(worker_key)
                        self._worker_browser_pool[worker_key] = browser
                        return browser
                
                return browser
            except Exception as e:
                logger.warning(f"Browser {worker_key} health check failed: {e}")
                await self._restart_browser(worker_key)
        
        # Create new browser
        browser = await self._create_new_browser(worker_key)
        self._worker_browser_pool[worker_key] = browser
        
        # Start keep-alive task for browser persistence
        if self._browser_persistence_enabled:
            self._browser_last_activity[worker_key] = time.time()
            self._browser_connection_status[worker_key] = "connected"
            keepalive_task = asyncio.create_task(self._browser_keepalive(worker_key))
            self._browser_keepalive_tasks[worker_key] = keepalive_task
            logger.info(f"Started keep-alive for browser {worker_key}")
        
        return browser
    
    async def _create_new_browser(self, worker_key: str):
        """Create new browser with optimized settings"""
        try:
            playwright = await async_playwright().start()
            
            # Enhanced browser arguments for stability and memory efficiency
            browser = await playwright.chromium.launch(
                headless=True,
                timeout=60000,  # 60 second timeout for browser launch
                args=[
                    # Essential stability
                    "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
                    
                    # Memory optimization
                    "--memory-pressure-off", "--max_old_space_size=1024", "--disable-gpu",
                    "--disable-background-timer-throttling", "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding", "--disable-background-networking",
                    
                    # Feature disabling for performance
                    "--disable-extensions", "--disable-default-apps", "--disable-sync",
                    "--disable-translate", "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                    "--disable-component-extensions-with-background-pages", "--disable-plugins-discovery",
                    "--disable-permissions-api", "--disable-presentation-api", "--disable-shared-workers",
                    "--disable-webgl", "--disable-webgl2",
                    
                    # Security and detection
                    "--disable-web-security", "--disable-client-side-phishing-detection",
                    "--disable-blink-features=AutomationControlled",
                    
                    # UI and audio
                    "--hide-scrollbars", "--mute-audio", "--no-first-run",
                    
                    # Logging and debugging
                    "--disable-logging", "--disable-gpu-logging", "--silent", "--log-level=3",
                    "--disable-crash-reporter", "--disable-in-process-stack-traces", "--disable-dev-tools",
                    
                    # Network and connectivity
                    "--disable-preconnect", "--disable-remote-fonts", "--disable-domain-reliability",
                    "--disable-component-update", "--no-report-upload",
                    
                    # System integration
                    "--use-mock-keychain", "--force-color-profile=srgb", "--metrics-recording-only",
                    
                    # Process management
                    "--no-zygote", "--disable-hang-monitor", "--disable-prompt-on-repost",
                    "--disable-xss-auditor", "--disable-breakpad",
                    
                    # Process isolation
                    f"--remote-debugging-port={9000 + hash(worker_key) % 5000}"
                ]
            )
            
            # Track browser creation
            self._last_restart[worker_key] = time.time()
            self._memory_usage[worker_key] = 0.0
            
            logger.info(f"Created new browser for worker {self._worker_id} with key {worker_key}")
            return browser
            
        except Exception as e:
            logger.error(f"Failed to create browser for worker {self._worker_id}: {e}")
            raise
    
    async def _restart_browser(self, worker_key: str):
        """Restart browser with proper cleanup"""
        try:
            # Stop keep-alive task
            if worker_key in self._browser_keepalive_tasks:
                keepalive_task = self._browser_keepalive_tasks[worker_key]
                keepalive_task.cancel()
                try:
                    await keepalive_task
                except asyncio.CancelledError:
                    pass
                del self._browser_keepalive_tasks[worker_key]
            
            if worker_key in self._worker_browser_pool:
                browser = self._worker_browser_pool[worker_key]
                await browser.close()
                del self._worker_browser_pool[worker_key]
            
            if worker_key in self._memory_usage:
                del self._memory_usage[worker_key]
            
            if worker_key in self._last_restart:
                del self._last_restart[worker_key]
            
            # Clean up persistence tracking
            if worker_key in self._browser_last_activity:
                del self._browser_last_activity[worker_key]
            
            if worker_key in self._browser_connection_status:
                del self._browser_connection_status[worker_key]
            
            # Force garbage collection
            await self._force_garbage_collection()
            
            logger.info(f"Restarted browser for worker {self._worker_id} with key {worker_key}")
            
        except Exception as e:
            logger.warning(f"Error restarting browser for worker {self._worker_id}: {e}")
    
    async def _restart_all_worker_browsers(self):
        """Restart all browsers for current worker"""
        try:
            worker_keys = [key for key in self._worker_browser_pool.keys() if key.startswith(self._worker_id)]
            for worker_key in worker_keys:
                await self._restart_browser(worker_key)
            
            logger.info(f"Restarted all browsers for worker {self._worker_id}")
            
        except Exception as e:
            logger.warning(f"Error restarting all browsers for worker {self._worker_id}: {e}")

    # Public API to control restart policy around critical batches
    async def suspend_restarts(self):
        """Prevent automatic restarts for this worker until resumed."""
        self._restart_suspended[self._worker_id] = True
        logger.info(f"Browser restarts suspended for worker {self._worker_id}")

    async def resume_restarts(self):
        """Allow automatic restarts again for this worker."""
        self._restart_suspended[self._worker_id] = False
        logger.info(f"Browser restarts resumed for worker {self._worker_id}")
    
    async def get_browser_status(self, crawler_id: str = None):
        """Get browser status information"""
        worker_key = f"{self._worker_id}_{crawler_id}" if crawler_id else None
        
        status = {
            "worker_id": self._worker_id,
            "browser_persistence_enabled": self._browser_persistence_enabled,
            "total_browsers": len(self._worker_browser_pool),
            "restart_suspended": self._restart_suspended.get(self._worker_id, False),
            "browsers": {}
        }
        
        for key, browser in self._worker_browser_pool.items():
            if worker_key and key != worker_key:
                continue
                
            browser_status = {
                "connected": browser.is_connected() if hasattr(browser, "is_connected") else "unknown",
                "last_activity": self._browser_last_activity.get(key, 0),
                "connection_status": self._browser_connection_status.get(key, "unknown"),
                "memory_usage_mb": self._memory_usage.get(key, 0),
                "has_keepalive": key in self._browser_keepalive_tasks
            }
            status["browsers"][key] = browser_status
        
        return status
    
    @asynccontextmanager
    async def get_playwright_context(self, crawler_id: str, user_agent: str, viewport: dict, task_id: str = None):
        """Enhanced playwright context with process isolation and memory monitoring"""
        context = None
        page = None
        
        try:
            async with self._lock:
                # Check worker memory pressure
                if await self._check_memory_pressure():
                    logger.warning(f"Memory pressure detected, forcing browser restart for worker {self._worker_id}")
                    await self._restart_all_worker_browsers()
                
                # Get browser with process isolation
                browser = await self._get_or_create_browser(crawler_id, task_id)
                
                # Check if we need to restart browser based on request count
                if task_id:
                    worker_key = f"{self._worker_id}_{crawler_id}_{task_id}"
                else:
                    worker_key = f"{self._worker_id}_{crawler_id}"
                if worker_key in self._request_counts:
                    self._request_counts[worker_key] += 1
                    if (not self._restart_suspended.get(self._worker_id, False)) and (self._request_counts[worker_key] > self._browser_restart_threshold):
                        logger.info(f"Restarting browser for worker {self._worker_id} after {self._request_counts[worker_key]} requests")
                        await self._restart_browser(worker_key)
                        browser = await self._get_or_create_browser(crawler_id)
                        self._request_counts[worker_key] = 0
                else:
                    self._request_counts[worker_key] = 1
                
                # Create context with retry logic and timeout
                context = None
                for attempt in range(3):
                    try:
                        context = await asyncio.wait_for(
                            browser.new_context(
                                user_agent=user_agent,
                                viewport=viewport,
                                extra_http_headers={
                                    'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
                                    'Accept-Encoding': 'gzip, deflate, br',
                                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                                    'Connection': 'keep-alive',
                                    'Upgrade-Insecure-Requests': '1',
                                }
                            ),
                            timeout=60
                        )
                        break
                    except asyncio.TimeoutError:
                        logger.warning(f"Context creation timeout for worker {self._worker_id} (attempt {attempt + 1}/3)")
                        if attempt < 2:
                            await asyncio.sleep(2)
                        else:
                            raise Exception("Context creation timeout after 3 attempts")
                    except Exception as e:
                        logger.warning(f"Failed to create context for worker {self._worker_id} (attempt {attempt + 1}/3): {e}")
                        if attempt < 2:
                            await asyncio.sleep(2)
                        else:
                            raise
                
                # Create page with retry logic and timeout
                page = None
                for attempt in range(3):
                    try:
                        page = await asyncio.wait_for(
                            context.new_page(),
                            timeout=30
                        )
                        break
                    except asyncio.TimeoutError:
                        logger.warning(f"Page creation timeout for worker {self._worker_id} (attempt {attempt + 1}/3)")
                        if attempt < 2:
                            await asyncio.sleep(2)
                        else:
                            # If page creation fails, recreate context
                            await context.close()
                            context = await browser.new_context(
                                user_agent=user_agent,
                                viewport=viewport,
                                extra_http_headers={
                                    'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
                                    'Accept-Encoding': 'gzip, deflate, br',
                                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                                    'Connection': 'keep-alive',
                                    'Upgrade-Insecure-Requests': '1',
                                }
                            )
                            page = await context.new_page()
                            break
                    except Exception as e:
                        logger.warning(f"Failed to create page for worker {self._worker_id} (attempt {attempt + 1}/3): {e}")
                        if attempt < 2:
                            await asyncio.sleep(2)
                        else:
                            raise
                
                # Track active contexts
                if worker_key not in self._active_contexts:
                    self._active_contexts[worker_key] = 0
                self._active_contexts[worker_key] += 1
                
                logger.debug(f"Created context for worker {self._worker_id}, active contexts: {self._active_contexts[worker_key]}")
                
                yield context, page
                
        except Exception as e:
            logger.error(f"Error in playwright context for worker {self._worker_id}: {e}")
            raise
        finally:
            # Cleanup context
            if context:
                try:
                    # Check if context is still valid before closing
                    if hasattr(context, '_connection') and context._connection:
                        await context.close()
                    else:
                        logger.debug(f"Context already closed for worker {self._worker_id}")
                except Exception as e:
                    error_msg = str(e)
                    if "Failed to find context" in error_msg or "Target.disposeBrowserContext" in error_msg:
                        logger.debug(f"Context already disposed for worker {self._worker_id}: {e}")
                    else:
                        logger.warning(f"Error closing context for worker {self._worker_id}: {e}")
                finally:
                    # Always update active contexts count
                    async with self._lock:
                        worker_key = f"{self._worker_id}_{crawler_id}"
                        if worker_key in self._active_contexts:
                            self._active_contexts[worker_key] = max(0, self._active_contexts[worker_key] - 1)
                    logger.debug(f"Context cleanup completed for worker {self._worker_id}, active contexts: {self._active_contexts.get(worker_key, 0)}")
    
    @asynccontextmanager
    async def get_crawl4ai_crawler(self, crawler_id: str, user_agent: str, viewport: dict = None):
        """Enhanced Crawl4AI crawler with process isolation and memory monitoring"""
        crawler = None
        
        try:
            async with self._lock:
                # Check worker memory pressure
                if await self._check_memory_pressure():
                    logger.warning(f"Memory pressure detected, forcing browser restart for worker {self._worker_id}")
                    await self._restart_all_worker_browsers()
                
                # Get or create crawler with process isolation
                crawler = await self._get_or_create_crawl4ai_crawler(crawler_id, user_agent, viewport)
                
                logger.debug(f"Created Crawl4AI crawler for worker {self._worker_id}")
                
                yield crawler
                
        except Exception as e:
            logger.error(f"Error in Crawl4AI crawler for worker {self._worker_id}: {e}")
            raise
        finally:
            # Cleanup crawler with better error handling
            if crawler:
                try:
                    # Check if crawler browser is still valid before closing
                    if hasattr(crawler, 'browser') and crawler.browser:
                        if hasattr(crawler.browser, '_connection') and crawler.browser._connection:
                            await crawler.close()
                        else:
                            logger.debug(f"Crawler browser already closed for worker {self._worker_id}")
                    else:
                        logger.debug(f"Crawler already closed for worker {self._worker_id}")
                except Exception as e:
                    error_msg = str(e)
                    if "Failed to find context" in error_msg or "Target.disposeBrowserContext" in error_msg or "has been closed" in error_msg:
                        logger.debug(f"Crawler already disposed for worker {self._worker_id}: {e}")
                    else:
                        logger.warning(f"Error closing Crawl4AI crawler for worker {self._worker_id}: {e}")
    
    async def _get_or_create_crawl4ai_crawler(self, crawler_id: str, user_agent: str, viewport: dict = None):
        """Get or create Crawl4AI crawler with process isolation"""
        worker_key = f"{self._worker_id}_{crawler_id}"
        
        # Check if crawler exists and is healthy
        if worker_key in self._worker_browser_pool:
            crawler = self._worker_browser_pool[worker_key]
            try:
                # Simple health check
                if hasattr(crawler, 'browser') and crawler.browser:
                    b = crawler.browser
                    if hasattr(b, "is_connected"):
                        if not b.is_connected():
                            raise Exception("Crawler browser disconnected")
                    else:
                        # Fallback safe version access
                        ver = None
                        try:
                            ver = await b.version()
                        except TypeError:
                            try:
                                ver = b.version
                            except Exception:
                                pass
                        if not ver:
                            raise Exception("Crawler browser health check failed: version unavailable")
                return crawler
            except Exception as e:
                logger.warning(f"Crawl4AI crawler {worker_key} health check failed: {e}")
                await self._restart_browser(worker_key)
        
        # Create new crawler
        crawler = await self._create_new_crawl4ai_crawler(worker_key, user_agent, viewport)
        self._worker_browser_pool[worker_key] = crawler
        return crawler
    
    async def _create_new_crawl4ai_crawler(self, worker_key: str, user_agent: str, viewport: dict = None):
        """Create new Crawl4AI crawler with optimized settings"""
        try:
            # Default viewport if not provided
            if viewport is None:
                viewport = {"width": 1920, "height": 1080}
            
            # Create crawler with process isolation
            crawler = AsyncWebCrawler(
                headless=True,
                user_agent=user_agent,
                viewport=viewport,
                browser_type="chromium",
                # Enhanced browser arguments for stability
                browser_args=[
                    "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
                    "--memory-pressure-off", "--max_old_space_size=1024", "--disable-gpu",
                    "--disable-background-timer-throttling", "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding", "--disable-background-networking",
                    "--disable-extensions", "--disable-default-apps", "--disable-sync",
                    "--disable-translate", "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                    "--disable-component-extensions-with-background-pages", "--disable-plugins-discovery",
                    "--disable-permissions-api", "--disable-presentation-api", "--disable-shared-workers",
                    "--disable-webgl", "--disable-webgl2", "--disable-web-security",
                    "--disable-client-side-phishing-detection", "--disable-blink-features=AutomationControlled",
                    "--hide-scrollbars", "--mute-audio", "--no-first-run", "--disable-logging",
                    "--disable-gpu-logging", "--silent", "--log-level=3", "--disable-crash-reporter",
                    "--disable-in-process-stack-traces", "--disable-dev-tools", "--disable-preconnect",
                    "--disable-remote-fonts", "--disable-domain-reliability", "--disable-component-update",
                    "--no-report-upload", "--use-mock-keychain", "--force-color-profile=srgb",
                    "--metrics-recording-only", "--no-zygote", "--disable-hang-monitor",
                    "--disable-prompt-on-repost", "--disable-xss-auditor", "--disable-breakpad",
                    f"--remote-debugging-port={8000 + hash(worker_key) % 5000}"
                ]
            )
            
            logger.info(f"Created new Crawl4AI crawler for worker {self._worker_id} with key {worker_key}")
            return crawler
            
        except Exception as e:
            logger.error(f"Failed to create Crawl4AI crawler for worker {self._worker_id}: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup all resources for current worker"""
        try:
            # Close all browsers for this worker
            await self._restart_all_worker_browsers()
            
            # Clear tracking data
            worker_keys = [key for key in self._request_counts.keys() if key.startswith(self._worker_id)]
            for key in worker_keys:
                del self._request_counts[key]
            
            worker_keys = [key for key in self._active_contexts.keys() if key.startswith(self._worker_id)]
            for key in worker_keys:
                del self._active_contexts[key]
            
            # Force garbage collection
            await self._force_garbage_collection()
            
            logger.info(f"Cleanup completed for worker {self._worker_id}")
            
        except Exception as e:
            logger.warning(f"Error during cleanup for worker {self._worker_id}: {e}")

# Create new instance for each worker (no singleton)
def get_context_manager():
    return AsyncBrowserContextManager()

# For backward compatibility, create instance
# context_manager = get_context_manager()
