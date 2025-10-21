"""
Proxy Configuration Manager for Broward Lis Pendens Automation
Handles selective proxy usage with IPRoyal residential proxies for ZabaSearch
"""

import os
import logging
from typing import Optional, Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.current_proxy_index = 0
        self.logger = logging.getLogger(__name__)
        self._load_proxy_config()

    def _load_proxy_config(self):
        """Load simplified proxy configuration - single rotating proxy"""
        try:
            # GEOGRAPHIC OPTIMIZATION: Use NYC instead of Atlanta for better routing
            # Server: Atlanta -> Proxy: NYC -> ZabaSearch: More diverse routing
            proxy_config = {
                'server': 'http://geo.iproyal.com:11201',
                'username': 'YCHKuA0Yy6LHLnT9',
                'password': 'sepsepani1_country-us_city-newyorkcity',  # Changed to NYC for optimal routing
                'randomizing_ip': True  # IPRoyal handles IP rotation automatically
            }

            self.proxies = [proxy_config]  # Single proxy entry
            self.logger.info(f"✅ Loaded 1 IPRoyal rotating proxy (handles IP rotation automatically)")

        except Exception as e:
            self.logger.error(f"❌ Error loading proxy config: {e}")

    def get_random_proxy(self) -> Optional[Dict]:
        """Get the single rotating proxy for ZabaSearch operations"""
        if not self.proxies:
            self.logger.info("🔄 No proxies available - using direct connection")
            return None

        # Return the single rotating proxy - IPRoyal handles IP rotation
        proxy = self.proxies[0]

        self.logger.info(f"🔒 Using IPRoyal rotating proxy: {proxy['server']}")
        return proxy

    def get_proxy_for_zabasearch(self) -> Optional[Dict]:
        """Get simplified proxy for ZabaSearch - IPRoyal handles IP rotation"""
        return self.get_random_proxy()  # Simple and effective

    def get_proxy_count(self) -> int:
        """Get number of configured proxies"""
        return len(self.proxies)

    def is_proxy_enabled(self) -> bool:
        """Check if any proxies are configured"""
        return len(self.proxies) > 0

# Global proxy manager instance
proxy_manager = ProxyManager()

def get_proxy_for_zabasearch() -> Optional[Dict]:
    """Get proxy specifically for ZabaSearch operations"""
    # Use the class method for consistency
    return proxy_manager.get_proxy_for_zabasearch()

def get_proxy_count() -> int:
    """Get number of configured proxies"""
    return proxy_manager.get_proxy_count()

def is_proxy_enabled() -> bool:
    """Check if proxies are configured"""
    return proxy_manager.is_proxy_enabled()

