"""Caching utilities for API responses."""

import time
from functools import wraps
from typing import Any, Callable, TypeVar, ParamSpec
from collections import OrderedDict
import hashlib
import json

P = ParamSpec("P")
R = TypeVar("R")


class TTLCache:
    """Simple TTL cache with max size limit."""

    def __init__(self, maxsize: int = 128, ttl: int = 3600):
        """Initialize cache.

        Args:
            maxsize: Maximum number of items to cache
            ttl: Time-to-live in seconds
        """
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def get(self, key: str) -> Any | None:
        """Get item from cache if not expired."""
        if key not in self._cache:
            return None

        timestamp, value = self._cache[key]
        if time.time() - timestamp > self.ttl:
            del self._cache[key]
            return None

        # Move to end (most recently used)
        self._cache.move_to_end(key)
        return value

    def set(self, key: str, value: Any) -> None:
        """Set item in cache."""
        # Remove oldest items if at capacity
        while len(self._cache) >= self.maxsize:
            self._cache.popitem(last=False)

        self._cache[key] = (time.time(), value)

    def clear(self) -> None:
        """Clear all cached items."""
        self._cache.clear()

    def invalidate(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern.

        Args:
            pattern: String pattern to match keys against

        Returns:
            Number of entries invalidated
        """
        keys_to_remove = [k for k in self._cache.keys() if pattern in k]
        for key in keys_to_remove:
            del self._cache[key]
        return len(keys_to_remove)


# Global cache instances
stats_cache = TTLCache(maxsize=50, ttl=300)  # 5 minutes for stats
manufacturer_cache = TTLCache(maxsize=100, ttl=1800)  # 30 minutes for manufacturer list
product_code_cache = TTLCache(maxsize=100, ttl=1800)  # 30 minutes for product codes
trends_cache = TTLCache(maxsize=50, ttl=600)  # 10 minutes for trends


def make_cache_key(*args: Any, **kwargs: Any) -> str:
    """Create a cache key from function arguments."""
    key_data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
    return hashlib.md5(key_data.encode()).hexdigest()


def cached(cache: TTLCache, key_prefix: str = "") -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator for caching function results.

    Args:
        cache: Cache instance to use
        key_prefix: Prefix for cache keys

    Returns:
        Decorated function
    """
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            cache_key = f"{key_prefix}:{make_cache_key(*args, **kwargs)}"

            # Try to get from cache
            cached_value = cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            # Compute and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result)
            return result

        return wrapper

    return decorator


def get_cache_stats() -> dict[str, dict[str, int]]:
    """Get statistics about cache usage."""
    return {
        "stats_cache": {
            "size": len(stats_cache._cache),
            "maxsize": stats_cache.maxsize,
            "ttl": stats_cache.ttl,
        },
        "manufacturer_cache": {
            "size": len(manufacturer_cache._cache),
            "maxsize": manufacturer_cache.maxsize,
            "ttl": manufacturer_cache.ttl,
        },
        "product_code_cache": {
            "size": len(product_code_cache._cache),
            "maxsize": product_code_cache.maxsize,
            "ttl": product_code_cache.ttl,
        },
        "trends_cache": {
            "size": len(trends_cache._cache),
            "maxsize": trends_cache.maxsize,
            "ttl": trends_cache.ttl,
        },
    }


def clear_all_caches() -> None:
    """Clear all cache instances."""
    stats_cache.clear()
    manufacturer_cache.clear()
    product_code_cache.clear()
    trends_cache.clear()
