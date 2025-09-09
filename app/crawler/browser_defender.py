"""
Browser Defender - Defensive programming to prevent TargetClosedError
"""
import asyncio
import logging
from typing import Any, Callable, Optional
from playwright._impl._errors import TargetClosedError

logger = logging.getLogger(__name__)

class BrowserDefender:
    """Defensive wrapper for browser operations to prevent TargetClosedError"""
    
    @staticmethod
    async def safe_browser_operation(
        operation: Callable,
        operation_name: str = "browser_operation",
        max_retries: int = 3,
        timeout: int = 30,
        cleanup_func: Optional[Callable] = None
    ) -> Any:
        """
        Safely execute browser operations with automatic retry and cleanup
        
        Args:
            operation: The async function to execute
            operation_name: Name for logging
            max_retries: Maximum number of retries
            timeout: Timeout for the operation
            cleanup_func: Optional cleanup function to call on failure
        """
        for attempt in range(max_retries + 1):
            try:
                # Execute operation with timeout
                result = await asyncio.wait_for(operation(), timeout=timeout)
                return result
                
            except TargetClosedError as e:
                logger.warning(f"{operation_name} failed with TargetClosedError (attempt {attempt + 1}/{max_retries + 1}): {e}")
                
                if attempt < max_retries:
                    # Call cleanup function if provided
                    if cleanup_func:
                        try:
                            await cleanup_func()
                        except Exception as cleanup_error:
                            logger.warning(f"Cleanup failed: {cleanup_error}")
                    
                    # Wait before retry
                    await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    logger.error(f"{operation_name} failed after {max_retries + 1} attempts")
                    raise
                    
            except asyncio.TimeoutError as e:
                logger.warning(f"{operation_name} timed out (attempt {attempt + 1}/{max_retries + 1}): {e}")
                
                if attempt < max_retries:
                    # Call cleanup function if provided
                    if cleanup_func:
                        try:
                            await cleanup_func()
                        except Exception as cleanup_error:
                            logger.warning(f"Cleanup failed: {cleanup_error}")
                    
                    # Wait before retry
                    await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    logger.error(f"{operation_name} timed out after {max_retries + 1} attempts")
                    raise
                    
            except Exception as e:
                error_str = str(e)
                if "Target page, context or browser has been closed" in error_str:
                    logger.warning(f"{operation_name} failed with browser closure (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    
                    if attempt < max_retries:
                        # Call cleanup function if provided
                        if cleanup_func:
                            try:
                                await cleanup_func()
                            except Exception as cleanup_error:
                                logger.warning(f"Cleanup failed: {cleanup_error}")
                        
                        # Wait before retry
                        await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
                        continue
                    else:
                        logger.error(f"{operation_name} failed after {max_retries + 1} attempts")
                        raise
                else:
                    # Other errors - don't retry
                    logger.error(f"{operation_name} failed with unexpected error: {e}")
                    raise
        
        # This should never be reached
        raise Exception(f"{operation_name} failed after all retries")
    
    @staticmethod
    async def safe_page_operation(
        page,
        operation: Callable,
        operation_name: str = "page_operation",
        max_retries: int = 2,
        timeout: int = 20
    ) -> Any:
        """
        Safely execute page operations with automatic retry
        """
        for attempt in range(max_retries + 1):
            try:
                # Check if page is still valid
                if page.is_closed():
                    raise TargetClosedError("Page is closed")
                
                # Execute operation with timeout
                result = await asyncio.wait_for(operation(), timeout=timeout)
                return result
                
            except (TargetClosedError, Exception) as e:
                error_str = str(e)
                if "Target page, context or browser has been closed" in error_str or "Page is closed" in error_str:
                    logger.warning(f"{operation_name} failed with page closure (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    
                    if attempt < max_retries:
                        await asyncio.sleep(1 * (attempt + 1))  # Short backoff
                        continue
                    else:
                        logger.error(f"{operation_name} failed after {max_retries + 1} attempts")
                        raise
                else:
                    # Other errors - don't retry
                    logger.error(f"{operation_name} failed with unexpected error: {e}")
                    raise
        
        # This should never be reached
        raise Exception(f"{operation_name} failed after all retries")
    
    @staticmethod
    async def safe_context_operation(
        context,
        operation: Callable,
        operation_name: str = "context_operation",
        max_retries: int = 2,
        timeout: int = 15
    ) -> Any:
        """
        Safely execute context operations with automatic retry
        """
        for attempt in range(max_retries + 1):
            try:
                # Execute operation with timeout
                result = await asyncio.wait_for(operation(), timeout=timeout)
                return result
                
            except (TargetClosedError, Exception) as e:
                error_str = str(e)
                if "Target page, context or browser has been closed" in error_str:
                    logger.warning(f"{operation_name} failed with context closure (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    
                    if attempt < max_retries:
                        await asyncio.sleep(1 * (attempt + 1))  # Short backoff
                        continue
                    else:
                        logger.error(f"{operation_name} failed after {max_retries + 1} attempts")
                        raise
                else:
                    # Other errors - don't retry
                    logger.error(f"{operation_name} failed with unexpected error: {e}")
                    raise
        
        # This should never be reached
        raise Exception(f"{operation_name} failed after all retries")
