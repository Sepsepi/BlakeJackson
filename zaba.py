"""
ZabaSearch Phone Number Extractor - Intelligent Batch Processor
Cross-references addresses from CSV with ZabaSearch data and extracts phone numbers
Features:
- Auto-detects latest CSV files with addresses
- Dynamic batch processing (configurable batch size)
- Command-line interface for automation
- Progress tracking and error recovery
- Rate limiting for respectful scraping
- Smart proxy management (proxies only for ZabaSearch, direct for AI)
"""
import asyncio
import pandas as pd
import random
import re
from playwright.async_api import async_playwright
from typing import Dict, List, Optional, Tuple
import time
import glob
import os
import gc
from urllib.parse import quote
import argparse

# Import our enhanced CSV format handler for intelligent address processing
try:
    from csv_format_handler import CSVFormatHandler
    print("✅ Enhanced CSV Format Handler loaded for intelligent address processing")
except ImportError as e:
    print(f"⚠️ CSV Format Handler not available: {e}")
    CSVFormatHandler = None

class ZabaSearchExtractor:
    def __init__(self, headless: bool = True):  # Default to headless
        self.headless = headless

        # 🚀 SERVER-OPTIMIZED TIMEOUTS for Proxy Environment
        self.navigation_timeout = int(os.environ.get('BROWARD_NAVIGATION_TIMEOUT', '18000'))   # 18 seconds for proxy routing
        self.selector_timeout = int(os.environ.get('BROWARD_SELECTOR_TIMEOUT', '12000'))       # 12 seconds for proxy delays
        self.agreement_timeout = int(os.environ.get('BROWARD_AGREEMENT_TIMEOUT', '18000'))     # 18 seconds for proxy routing

        # ATLANTA-CONSISTENT: Windows-only, latest Firefox, matches proxy location
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
            'Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0',
            'Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0'
        ]
        # Use same agents for both - consistency is key
        self.firefox_user_agents = self.user_agents
        self.terms_accepted = False
        self.blocked_requests = []  # Track blocked requests for monitoring
        self.allowed_requests = []  # Track allowed requests for comparison

    async def enhanced_route_handler(self, route):
        """PHONE-ONLY Enhanced route handler - MAXIMUM bandwidth optimization for phone extraction only"""
        request = route.request
        url = request.url.lower()
        resource_type = request.resource_type

        # Essential domains that must be allowed for ZabaSearch functionality
        essential_domains = [
            'zabasearch.com',
            'intelius.com',
            'secure.zabasearch.com',
            'www.zabasearch.com'
        ]

        # 🚫 PHONE-ONLY: Block unnecessary data sections that waste bandwidth
        # Block requests for emails, relatives, job history, education data
        unwanted_data_patterns = [
            'email', 'relatives', 'family', 'education', 'job', 'employment',
            'social', 'linkedin', 'facebook', 'twitter', 'instagram',
            'background-check', 'criminal', 'court', 'bankruptcy',
            'assets', 'property', 'business', 'companies', 'employers',
            'schools', 'universities', 'degrees', 'certifications',
            'marriages', 'divorces', 'relationships', 'associations',
            'profile-images', 'photos', 'pictures', 'avatars'
        ]
        
        if any(pattern in url for pattern in unwanted_data_patterns):
            self.blocked_requests.append(f"UNWANTED_DATA: {url[:100]}")
            await route.abort()
            return

        # ULTRA-AGGRESSIVE: Block ALL non-essential domains completely
        third_party_domains = [
            'cdn.', 'static.', 'assets.', 'libs.', 'ajax.googleapis.com',
            'code.jquery.com', 'stackpath.bootstrapcdn.com', 'unpkg.com',
            'jsdelivr.net', 'cdnjs.cloudflare.com', 'maxcdn.bootstrapcdn.com',
            'googlesyndication.com', 'doubleclick.net', 'amazon-adsystem.com',
            'facebook.net', 'twitter.com', 'instagram.com', 'linkedin.com',
            'youtube.com', 'vimeo.com', 'tiktok.com', 'snapchat.com', 
            'pinterest.com', 'reddit.com', 'tumblr.com'
        ]
        
        if any(domain in url for domain in third_party_domains):
            self.blocked_requests.append(f"THIRD_PARTY_CDN: {url[:100]}")
            await route.abort()
            return

        # ULTRA-AGGRESSIVE: Block even more resource types
        blocked_resource_types = [
            'websocket', 'eventsource', 'manifest', 'texttrack', 'sub_frame'
        ]
        
        if resource_type in blocked_resource_types:
            self.blocked_requests.append(f"BLOCKED_RESOURCE_TYPE: {resource_type} - {url[:100]}")
            await route.abort()
            return

        # PHONE-ONLY: Block ALL images and media (including profile photos, icons)
        if resource_type in ['image', 'media']:
            self.blocked_requests.append(f"ALL_MEDIA_BLOCKED: {url[:100]}")
            await route.abort()
            return

        # SMART CSS BLOCKING: Allow minimal form CSS, block decorative styling
        if resource_type == 'stylesheet':
            # Allow only critical form interaction CSS
            form_critical_css = ['form', 'input', 'button', 'search', 'textbox', 'dropdown', 'select']
            if any(essential_domain in url for essential_domain in essential_domains) and \
               any(form_css in url for form_css in form_critical_css):
                self.allowed_requests.append(f"FORM_CSS_ALLOWED: {url[:100]}")
                await route.continue_()
                return
            else:
                self.blocked_requests.append(f"DECORATIVE_CSS_BLOCKED: {url[:100]}")
                await route.abort()
                return

        # ULTRA-STRICT JS BLOCKING: Only essential form/navigation scripts
        if resource_type == 'script':
            # Tightened to absolutely essential only
            critical_js = ['submit', 'csrf', 'form', 'search']  # Removed 'captcha', 'phone' for max blocking
            tracking_js = ['analytics', 'tracking', 'gtag', 'fbpixel', 'hotjar', 'segment', 'mixpanel']
            
            # Block all tracking scripts first
            if any(track in url for track in tracking_js):
                self.blocked_requests.append(f"TRACKING_JS_BLOCKED: {url[:100]}")
                await route.abort()
                return
            
            # Allow only critical navigation scripts from ZabaSearch domains
            if any(essential_domain in url for essential_domain in essential_domains) and \
               any(critical in url for critical in critical_js):
                self.allowed_requests.append(f"CRITICAL_JS_ALLOWED: {url[:100]}")
                await route.continue_()
                return
            else:
                self.blocked_requests.append(f"NON_CRITICAL_JS_BLOCKED: {url[:100]}")
                await route.abort()
                return

        # PHONE-ONLY: Block analytics and tracking (massive bandwidth waste)
        analytics_domains = [
            'google-analytics.com', 'googletagmanager.com', 'googlesyndication.com',
            'doubleclick.net', 'googleadservices.com', 'amazon-adsystem.com',
            'cookieyes.com', 'contributor.google.com', 'adtrafficquality.google',
            'fundingchoicesmessages.google.com', 'js-sec.indexww.com',
            'facebook.com/tr', 'connect.facebook.net', 'analytics.twitter.com',
            'scorecardresearch.com', 'quantserve.com', 'hotjar.com',
            'crazyegg.com', 'fullstory.com', 'segment.com', 'mixpanel.com'
        ]

        if any(domain in url for domain in analytics_domains):
            self.blocked_requests.append(f"ANALYTICS_BLOCKED: {url[:100]}")
            await route.abort()
            return

        # PHONE-ONLY: Block fonts (unnecessary for phone extraction)
        if any(font_url in url for font_url in ['fonts.googleapis.com', 'fonts.gstatic.com', 'typekit.net', 'fontawesome']):
            self.blocked_requests.append(f"FONTS_BLOCKED: {url[:100]}")
            await route.abort()
            return

        # PHONE-ONLY: Block fonts resource type
        if resource_type == 'font':
            self.blocked_requests.append(f"FONT_RESOURCE_BLOCKED: {url[:100]}")
            await route.abort()
            return

        # PHONE-ONLY: Block WebRTC and multimedia streams
        if any(webrtc in url for webrtc in ['webrtc', 'stun:', 'turn:', 'rtc']):
            self.blocked_requests.append(f"WEBRTC_BLOCKED: {url[:100]}")
            await route.abort()
            return

        # PHONE-ONLY: Block tracking and beacon requests
        if resource_type in ['beacon', 'other', 'ping']:
            self.blocked_requests.append(f"TRACKING_BLOCKED: {resource_type} - {url[:100]}")
            await route.abort()
            return

        # PHONE-ONLY: Block social widgets and ads
        social_ad_patterns = [
            'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
            'ads', 'advertisement', 'sponsored', 'promo', 'banner',
            'social-share', 'like-button', 'tweet-button'
        ]

        if any(pattern in url for pattern in social_ad_patterns):
            if not any(essential_domain in url for essential_domain in essential_domains):
                self.blocked_requests.append(f"SOCIAL_ADS_BLOCKED: {url[:100]}")
                await route.abort()
                return

        # RESULTS PAGE OPTIMIZATION: Block everything except text content on results pages
        if 'results' in url or 'search' in url or 'person' in url:
            if resource_type in ['image', 'stylesheet', 'font', 'media']:
                self.blocked_requests.append(f"RESULTS_PAGE_VISUAL_BLOCKED: {resource_type} - {url[:100]}")
                await route.abort()
                return

        # � PHONE-ONLY: Allow only essential resources for phone extraction
        # CRITICAL: Allow only essential resource types for phone data extraction
        critical_resources = ['document', 'xhr', 'fetch']  # Only page HTML and phone data requests

        if resource_type in critical_resources:
            # Allow only phone-related requests from ZabaSearch
            if any(essential_domain in url for essential_domain in essential_domains):
                # Extra check: block if URL contains unwanted data endpoints
                if any(unwanted in url for unwanted in unwanted_data_patterns):
                    self.blocked_requests.append(f"UNWANTED_ENDPOINT_BLOCKED: {url[:100]}")
                    await route.abort()
                    return
                    
                self.allowed_requests.append(f"PHONE_CRITICAL: {resource_type} - {url[:100]}")
                await route.continue_()
                return

        # BLOCK ALL OTHER RESOURCE TYPES - maximum bandwidth saving for phone extraction only
        self.blocked_requests.append(f"ULTRA_BLOCKED: {resource_type} - {url[:100]}")
        await route.abort()

    def print_bandwidth_stats(self):
        """Print PHONE-ONLY MAXIMUM bandwidth optimization statistics"""
        total_requests = len(self.blocked_requests) + len(self.allowed_requests)
        if total_requests > 0:
            blocked_percentage = (len(self.blocked_requests) / total_requests) * 100
            print(f"\n📞 PHONE-ONLY MAXIMUM BANDWIDTH OPTIMIZATION STATS:")
            print(f"   🚫 BLOCKED: {len(self.blocked_requests)} requests ({blocked_percentage:.1f}%)")
            print(f"   ✅ ALLOWED: {len(self.allowed_requests)} requests ({100-blocked_percentage:.1f}%)")
            print(f"   💾 MAXIMUM bandwidth saved: {blocked_percentage:.0f}% (Target: 95%+ reduction)")
            print(f"   📞 Functionality: PHONE EXTRACTION ONLY - all other data blocked")
            
            if blocked_percentage >= 95:
                print(f"   🎯 EXCELLENT: Bandwidth optimization target achieved!")
            elif blocked_percentage >= 85:
                print(f"   ✅ GOOD: Strong bandwidth optimization in effect")
            else:
                print(f"   ⚠️  MODERATE: Room for further optimization")

            # Show breakdown of what was blocked
            unwanted_data = sum(1 for req in self.blocked_requests if 'UNWANTED_DATA' in req)
            media_blocked = sum(1 for req in self.blocked_requests if 'ALL_MEDIA_BLOCKED' in req)
            css_blocked = sum(1 for req in self.blocked_requests if 'CSS_BLOCKED' in req)
            js_blocked = sum(1 for req in self.blocked_requests if 'JS_BLOCKED' in req)
            analytics_blocked = sum(1 for req in self.blocked_requests if 'ANALYTICS' in req)
            ultra_blocked = sum(1 for req in self.blocked_requests if 'ULTRA_BLOCKED' in req)

            print(f"   📊 PHONE-ONLY BLOCKING BREAKDOWN:")
            if ultra_blocked > 0:
                print(f"     � Ultra-blocked everything else: {ultra_blocked}")
            if media_blocked > 0:
                print(f"     🖼️  ALL media blocked: {media_blocked}")
            if css_blocked > 0:
                print(f"     🎨 Heavy CSS blocked: {css_blocked}")
            if js_blocked > 0:
                print(f"     ⚡ Heavy JS blocked: {js_blocked}")

            # Show what minimal requests were allowed
            phone_critical = sum(1 for req in self.allowed_requests if 'PHONE_CRITICAL' in req)
            phone_js = sum(1 for req in self.allowed_requests if 'PHONE_ESSENTIAL_JS' in req)
            critical_allowed = sum(1 for req in self.allowed_requests if 'CRITICAL' in req)
            essential_allowed = sum(1 for req in self.allowed_requests if 'ESSENTIAL_MINIMAL' in req)

            print(f"   ✅ PHONE-ONLY ALLOWANCES:")
            if critical_allowed > 0:
                print(f"     � Critical docs/forms: {critical_allowed}")
            if essential_allowed > 0:
                print(f"     ⚙️  Essential minimal: {essential_allowed}")
            if phone_critical > 0:
                print(f"     📞 Phone data requests: {phone_critical}")
            if phone_js > 0:
                print(f"     ⚙️  Phone-essential JS: {phone_js}")

            # Estimate bandwidth savings for phone-only extraction
            estimated_saved_kb = blocked_percentage * 2  # Rough estimate: each blocked request saves ~2KB on average
            print(f"   💰 Estimated bandwidth saved per search: ~{estimated_saved_kb:.0f}KB")
            print(f"   🎯 Data extracted: PHONE NUMBERS ONLY (no emails, ages, relatives, jobs)")

        # Reset for next search
        self.blocked_requests = []
        self.allowed_requests = []

    async def create_stealth_browser(self, playwright, browser_type='chromium', proxy=None):
        """Create a browser with ADVANCED stealth capabilities, complete session isolation, and proxy failover"""

        # Convert proxy format for Playwright compatibility
        playwright_proxy = None
        if proxy:
            playwright_proxy = {
                'server': proxy['server']
            }
            if 'username' in proxy and 'password' in proxy:
                playwright_proxy['username'] = proxy['username']
                playwright_proxy['password'] = proxy['password']

        # Generate completely random session data for each batch
        session_id = random.randint(100000, 999999)
        print(f"🆔 Creating new browser session #{session_id} with isolated fingerprint")

        # ATLANTA-CONSISTENT: Common US business resolutions (matches professional users)
        viewports = [
            {'width': 1920, 'height': 1080},  # Full HD - most common
            {'width': 1366, 'height': 768},   # Standard laptop
            {'width': 1440, 'height': 900},   # Business laptop
            {'width': 1600, 'height': 900},   # Widescreen business
            {'width': 1920, 'height': 1080},  # Duplicate for higher chance
            {'width': 1366, 'height': 768}    # Duplicate for higher chance
        ]
        viewport = random.choice(viewports)

        # Fixed Eastern timezone - matches IPRoyal NYC proxy and server location
        atlanta_timezone = "America/New_York"  # Eastern Time (works for both Atlanta server and NYC proxy)

        print(f"🖥️ Viewport: {viewport['width']}x{viewport['height']}")
        print(f"🕐 Timezone: {atlanta_timezone} (Eastern Time - Server & Proxy compatible)")
        if proxy:
            print(f"🔒 Using proxy: {proxy['server']}")

        if browser_type == 'firefox':
            # FIXED: Firefox-compatible args (removed Chrome-specific arguments)
            launch_args = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--new-instance',
                '--no-remote',
                '--no-first-run'
            ]

            # Firefox-specific preferences for bandwidth optimization + PROXY FIX
            firefox_prefs = {
                # Disable prefetching to save bandwidth
                'network.prefetch-next': False,
                'network.dns.disablePrefetch': True,
                'network.http.speculative-parallel-limit': 0,

                # Disable WebRTC for privacy and bandwidth
                'media.peerconnection.enabled': False,
                'media.navigator.enabled': False,

                # Optimize image loading
                'browser.display.use_document_fonts': 0,  # Don't load external fonts

                # Disable auto-updates and telemetry
                'app.update.enabled': False,
                'toolkit.telemetry.enabled': False,
                'datareporting.healthreport.uploadEnabled': False,

                # Performance optimizations
                'browser.sessionstore.resume_from_crash': False,
                'browser.sessionstore.restore_on_demand': False,
                'browser.cache.disk.enable': False,  # Disable disk cache for speed
                'browser.cache.memory.enable': True,   # Use memory cache only

                # Disable unnecessary features
                'geo.enabled': False,
                'browser.safebrowsing.enabled': False,
                'browser.safebrowsing.malware.enabled': False,

                # 🔧 PROXY COMPATIBILITY FIXES for IPRoyal
                'network.proxy.allow_hijacking_localhost': True,
                'network.proxy.share_proxy_settings': True,
                'network.automatic-ntlm-auth.allow-proxies': True,
                'network.negotiate-auth.allow-proxies': True,
                'security.tls.insecure_fallback_hosts': 'geo.iproyal.com',
                'network.stricttransportsecurity.preloadlist': False,
                'security.mixed_content.block_active_content': False,
                'security.mixed_content.block_display_content': False,
                'network.proxy.failover_timeout': int(os.environ.get('SERVER_PROXY_TIMEOUT', '30000')) // 1000,
                'network.http.connection-timeout': 25,
                'network.http.response.timeout': 60
            }

            browser = await playwright.firefox.launch(
                headless=self.headless,
                args=launch_args,
                proxy=playwright_proxy,
                firefox_user_prefs=firefox_prefs  # Apply Firefox-specific optimizations
            )

            context = await browser.new_context(
                viewport=viewport,
                user_agent=random.choice(self.firefox_user_agents),
                locale='en-US',
                timezone_id=atlanta_timezone,
                device_scale_factor=random.choice([1, 1.25, 1.5]),
                has_touch=random.choice([True, False]),
                permissions=['geolocation'],
                geolocation={'longitude': random.uniform(-84.5, -84.3), 'latitude': random.uniform(33.6, 33.8)},
                java_script_enabled=True,
                bypass_csp=True,
                ignore_https_errors=True
            )

            # Set default timeouts for all page operations
            context.set_default_timeout(self.navigation_timeout)
            context.set_default_navigation_timeout(self.navigation_timeout)

            # 🚀 ULTRA-AGGRESSIVE BANDWIDTH CONTROL - Apply to Firefox!
            await context.route("**/*", self.enhanced_route_handler)
            print(f"🚫 ULTRA-AGGRESSIVE BANDWIDTH SAVER: Applied to Firefox (85%+ bandwidth reduction)")

            # 🚀 SMART POPUP/AD DESTRUCTION + STEALTH MODE
            await context.add_init_script("""
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });

                // Remove automation indicators
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

                // 🚀 SMART POPUP/AD DESTRUCTION - Preserve ZabaSearch functionality
                const smartPopupDestruction = () => {
                    // TARGETED destruction - avoid ZabaSearch essential elements
                    const destructionSelectors = [
                        // Only target obvious ads and external popups
                        '.google-ad', '.doubleclick', '.sponsored', '.promotion',
                        '[class*="advertisement"]', '[class*="ads-"]', '[class*="adsense"]',
                        // Social widgets (not needed for ZabaSearch)
                        '[class*="facebook"]', '[class*="twitter"]', '[class*="social"]',
                        '[class*="share"]', '[class*="like"]', '[class*="follow"]',
                        // External subscription popups (not ZabaSearch terms)
                        '.subscription-popup', '.newsletter-popup',
                        '[class*="subscribe"]', '[class*="newsletter"]',
                        // Tracking and analytics elements
                        '[class*="tracking"]', '[class*="analytics"]', '[class*="pixel"]'
                    ];

                    destructionSelectors.forEach(selector => {
                        try {
                            const elements = document.querySelectorAll(selector);
                            elements.forEach(el => {
                                // Extra check: Don't destroy if it contains "AGREE" or ZabaSearch terms
                                const text = (el.textContent || '').toLowerCase();
                                if (!text.includes('agree') &&
                                    !text.includes('terms') &&
                                    !text.includes('zabasearch') &&
                                    !text.includes('search')) {
                                    el.style.display = 'none !important';
                                    try { el.remove(); } catch(e) {}
                                }
                            });
                        } catch(e) {}
                    });

                    // Only target high z-index elements that are clearly ads (not terms dialogs)
                    const suspiciousElements = document.querySelectorAll('*');
                    suspiciousElements.forEach(el => {
                        try {
                            const styles = window.getComputedStyle(el);
                            const zIndex = parseInt(styles.zIndex) || 0;
                            const text = (el.textContent || '').toLowerCase();
                            const className = (el.className || '').toLowerCase();

                            // Only destroy if it's clearly an ad AND doesn't contain essential ZabaSearch content
                            if (zIndex > 2000 &&
                                (className.includes('ad') || text.includes('advertisement')) &&
                                !text.includes('agree') &&
                                !text.includes('terms') &&
                                !text.includes('zabasearch')) {
                                el.style.display = 'none !important';
                                try { el.remove(); } catch(e) {}
                            }
                        } catch(e) {}
                    });

                    // PRESERVE and enhance ZabaSearch form elements
                    const essentialElements = document.querySelectorAll(
                        'input, select, button, textarea, form, a, [role="button"]'
                    );
                    essentialElements.forEach(el => {
                        el.style.pointerEvents = 'auto !important';
                        el.style.visibility = 'visible !important';
                        el.style.opacity = '1 !important';
                        el.style.display = 'block !important';
                        el.removeAttribute('disabled');
                        el.removeAttribute('readonly');
                    });
                };

                // NUCLEAR POPUP CREATION BLOCKING
                const originalCreateElement = document.createElement;
                document.createElement = function(tagName) {
                    const element = originalCreateElement.call(this, tagName);
                    if (tagName.toLowerCase() === 'div' || tagName.toLowerCase() === 'iframe') {
                        const originalSetAttribute = element.setAttribute;
                        element.setAttribute = function(name, value) {
                            if (name === 'class' || name === 'id') {
                                const val = (value || '').toLowerCase();
                                if (val.includes('modal') || val.includes('popup') ||
                                    val.includes('overlay') || val.includes('ad') ||
                                    val.includes('banner') || val.includes('subscribe')) {
                                    console.log('🚫 BLOCKED popup/ad creation:', value);
                                    return; // Block the creation
                                }
                            }
                            return originalSetAttribute.call(this, name, value);
                        };
                    }
                    return element;
                };

                // Block common popup/modal functions
                window.open = function() { return null; };
                window.showModalDialog = function() { return null; };
                window.alert = function() { return null; };
                window.confirm = function() { return true; };

                // Execute smart destruction immediately but less aggressively
                smartPopupDestruction();
                document.addEventListener('DOMContentLoaded', smartPopupDestruction);

                // MODERATE monitoring for new popups/ads (less aggressive)
                const observer = new MutationObserver(() => {
                    smartPopupDestruction();
                });
                observer.observe(document.body || document.documentElement, {
                    childList: true,
                    subtree: true
                });

                // Reduced frequency destruction - every 2 seconds instead of 500ms
                setInterval(smartPopupDestruction, 2000);
                """)
            print(f"🛡️ STEALTH MODE: Anti-detection + smart popup destruction active")

        else:  # chromium - ENHANCED WITH BANDWIDTH OPTIMIZATION
            # Enhanced Chrome args with maximum stealth + BANDWIDTH OPTIMIZATION
            launch_args = [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--disable-features=VizDisplayCompositor',
                '--disable-background-networking',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-backgrounding-occluded-windows',
                '--disable-ipc-flooding-protection',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-translate',
                '--disable-default-apps',
                '--disable-web-security',
                '--disable-features=TranslateUI',
                '--disable-blink-features=AutomationControlled',
                '--no-default-browser-check',
                '--disable-component-extensions-with-background-pages',
                '--disable-background-mode',
                '--disable-client-side-phishing-detection',
                '--disable-sync',
                '--disable-features=Translate',
                '--enable-unsafe-swiftshader',
                '--use-mock-keychain',
                '--disable-popup-blocking',
                '--start-maximized',

                # 🚀 BANDWIDTH OPTIMIZATION FLAGS (96% reduction potential)
                '--disable-images',                           # Block all images (-50-70% bandwidth)
                '--disable-javascript-harmony-shipping',      # Reduce JS processing
                '--disable-remote-fonts',                     # Block Google Fonts (-6% bandwidth)
                '--disable-background-media-suspend',         # Prevent media loading
                '--disable-media-session-api',                # Block media APIs
                '--disable-presentation-api',                 # Block presentation APIs
                '--disable-reading-from-canvas',              # Reduce canvas operations
                '--disable-shared-workers',                   # Block shared worker scripts
                '--disable-speech-api',                       # Block speech APIs
                '--disable-file-system',                      # Block filesystem APIs
                '--disable-sensors',                          # Block sensor APIs
                '--disable-notifications',                    # Block notification APIs
                '--disable-geolocation',                      # Block geolocation (we set manually)
                '--autoplay-policy=user-gesture-required',    # Block autoplay media
                '--disable-domain-reliability',               # Block telemetry
                '--disable-features=AudioServiceOutOfProcess', # Reduce audio processing
                '--disable-features=MediaRouter',             # Block media router
                '--blink-settings=imagesEnabled=false',       # Force disable images in Blink

                '--user-agent=' + random.choice(self.user_agents)
            ]

            browser = await playwright.chromium.launch(
                headless=self.headless,
                args=launch_args,
                proxy=playwright_proxy
            )
            print(f"🚀 DEBUG: Browser launched successfully with proxy: {playwright_proxy['server'] if playwright_proxy else 'None'}")

            # Enhanced context creation with better error handling
            try:
                context = await browser.new_context(
                    viewport=viewport,
                    user_agent=random.choice(self.firefox_user_agents),
                    locale='en-US',
                    timezone_id=atlanta_timezone,
                    screen={'width': viewport['width'], 'height': viewport['height']},
                    device_scale_factor=random.choice([1, 1.25, 1.5]),
                    has_touch=random.choice([True, False]),
                    is_mobile=False,
                    permissions=['geolocation'],
                    geolocation={'longitude': random.uniform(-84.5, -84.3), 'latitude': random.uniform(33.6, 33.8)},
                    # Enhanced connection stability settings
                    extra_http_headers={
                        'Connection': 'keep-alive',
                        'Keep-Alive': 'timeout=30, max=100',
                        'Cache-Control': 'no-cache',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate',
                        'DNT': '1',
                        'Upgrade-Insecure-Requests': '1'
                    }
                )
                print(f"✅ Browser context created successfully")
            except Exception as e:
                print(f"❌ Error creating browser context: {e}")
                # Try with minimal settings as fallback
                context = await browser.new_context(
                    viewport=viewport,
                    user_agent=random.choice(self.user_agents)
                )
                print(f"🔄 Fallback context created successfully")

            # Continue with existing context setup
            context.set_extra_http_headers({
                'Connection': 'keep-alive',
                'Keep-Alive': 'timeout=30, max=100'
            })

            # Set default timeouts for all page operations
            context.set_default_timeout(self.navigation_timeout)
            context.set_default_navigation_timeout(self.navigation_timeout)
            print(f"⏱️ DEBUG: Timeouts set - navigation: {self.navigation_timeout}ms")

            # 🚀 ENHANCED BANDWIDTH CONTROL - Smart selective blocking preserving functionality
            await context.route("**/*", self.enhanced_route_handler)
            print(f"🚫 ENHANCED BANDWIDTH SAVER: Smart blocking (70-80% bandwidth reduction, all functionality preserved)")

        # ADVANCED ANTI-DETECTION + AD-BLOCKING SCRIPTS FOR BOTH BROWSERS
        await context.add_init_script("""
            // Remove webdriver traces
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });

            // Mock plugins with realistic data
            Object.defineProperty(navigator, 'plugins', {
                get: () => ({
                    length: 3,
                    0: { name: 'Chrome PDF Plugin', description: 'Portable Document Format', filename: 'internal-pdf-viewer' },
                    1: { name: 'Chrome PDF Viewer', description: '', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                    2: { name: 'Native Client', description: '', filename: 'internal-nacl-plugin' }
                }),
            });

            // 🚀 ENHANCED SMART AD & BANDWIDTH BLOCKING (preserves I Agree button and forms)
            // Block major ad networks and tracking but preserve ZabaSearch functionality
            const blockedDomains = [
                'googlesyndication.com', 'doubleclick.net', 'googleadservices.com',
                'amazon-adsystem.com', 'adsrvr.org', 'rlcdn.com', 'casalemedia.com',
                'pubmatic.com', 'adnxs.com', 'google-analytics.com', 'googletagmanager.com',
                'cookieyes.com', 'securepubads.g.doubleclick.net', 'pagead2.googlesyndication.com',
                'fundingchoicesmessages.google.com', 'contributor.google.com', 'adtrafficquality.google',
                'js-sec.indexww.com'
            ];

            // Enhanced fetch blocking that preserves essential site functions
            const originalFetch = window.fetch;
            window.fetch = function(...args) {
                const url = args[0];
                if (typeof url === 'string') {
                    // Block analytics and ads
                    if (blockedDomains.some(domain => url.includes(domain))) {
                        console.log('🚫 Blocked fetch:', url);
                        return Promise.reject(new Error('Blocked by enhanced filter'));
                    }
                    // Block profile images (heavy bandwidth)
                    if (url.includes('spd-assets.zabasearch.com/image/') ||
                        url.includes('profile-images.instantcheckmate.com')) {
                        console.log('🚫 Blocked image:', url);
                        return Promise.reject(new Error('Blocked profile image'));
                    }
                }
                    console.log('🚫 Blocked ad request:', url);
                    return Promise.reject(new Error('Blocked by ad blocker'));
                }
                return originalFetch.apply(this, args);
            };

            // Override XMLHttpRequest to block tracking
            const originalXHROpen = XMLHttpRequest.prototype.open;
            XMLHttpRequest.prototype.open = function(method, url, ...args) {
                if (typeof url === 'string' && blockedDomains.some(domain => url.includes(domain))) {
                    console.log('🚫 Blocked XHR request:', url);
                    throw new Error('Blocked by ad blocker');
                }
                return originalXHROpen.apply(this, [method, url, ...args]);
            };

            // Block image loading for ads
            const originalCreateElement = document.createElement;
            document.createElement = function(tagName) {
                const element = originalCreateElement.call(this, tagName);
                if (tagName.toLowerCase() === 'img') {
                    const originalSrc = element.src;
                    Object.defineProperty(element, 'src', {
                        get: () => originalSrc,
                        set: (value) => {
                            if (typeof value === 'string' && blockedDomains.some(domain => value.includes(domain))) {
                                console.log('🚫 Blocked image:', value);
                                return;
                            }
                            element.setAttribute('src', value);
                        }
                    });
                }
                return element;
            };
        """)

        # Additional stealth scripts
        await context.add_init_script("""
            // Realistic language settings
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });

            // Mock Chrome runtime
            window.chrome = {
                runtime: {
                    onConnect: undefined,
                    onMessage: undefined
                },
                app: {
                    isInstalled: false
                }
            };

            // Canvas fingerprint randomization
            const getImageData = HTMLCanvasElement.prototype.getContext('2d').getImageData;
            HTMLCanvasElement.prototype.getContext('2d').getImageData = function(...args) {
                const result = getImageData.apply(this, args);
                // Add tiny noise to canvas
                for (let i = 0; i < result.data.length; i += 4) {
                    result.data[i] += Math.floor(Math.random() * 3) - 1;
                    result.data[i + 1] += Math.floor(Math.random() * 3) - 1;
                    result.data[i + 2] += Math.floor(Math.random() * 3) - 1;
                }
                return result;
            };

            // WebRTC IP leak protection
            const RTCPeerConnection = window.RTCPeerConnection || window.mozRTCPeerConnection || window.webkitRTCPeerConnection;
            if (RTCPeerConnection) {
                const originalCreateDataChannel = RTCPeerConnection.prototype.createDataChannel;
                RTCPeerConnection.prototype.createDataChannel = function() {
                    return originalCreateDataChannel.apply(this, arguments);
                };
            }

            // Audio context fingerprint randomization
            const AudioContext = window.AudioContext || window.webkitAudioContext;
            if (AudioContext) {
                const originalCreateAnalyser = AudioContext.prototype.createAnalyser;
                AudioContext.prototype.createAnalyser = function() {
                    const analyser = originalCreateAnalyser.apply(this, arguments);
                    const originalGetByteFrequencyData = analyser.getByteFrequencyData;
                    analyser.getByteFrequencyData = function(array) {
                        originalGetByteFrequencyData.apply(this, arguments);
                        // Add slight noise
                        for (let i = 0; i < array.length; i++) {
                            array[i] += Math.floor(Math.random() * 3) - 1;
                        }
                    };
                    return analyser;
                };
            }

            // Screen resolution noise
            const originalScreen = window.screen;
            Object.defineProperties(window.screen, {
                width: { value: originalScreen.width + Math.floor(Math.random() * 3) - 1 },
                height: { value: originalScreen.height + Math.floor(Math.random() * 3) - 1 },
                availWidth: { value: originalScreen.availWidth + Math.floor(Math.random() * 3) - 1 },
                availHeight: { value: originalScreen.availHeight + Math.floor(Math.random() * 3) - 1 }
            });

            // Battery API spoofing
            if (navigator.getBattery) {
                navigator.getBattery = () => Promise.resolve({
                    charging: true,
                    chargingTime: Infinity,
                    dischargingTime: Infinity,
                    level: Math.random()
                });
            }

            // Remove automation indicators
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

            console.log('🛡️ Advanced stealth mode activated');
        """)

        print(f"🛡️ Advanced anti-detection measures activated for session #{session_id}")
        return browser, context

    async def human_delay(self, delay_type="normal"):
        """SERVER-OPTIMIZED delays - Proper timing for proxy/server environment"""
        delays = {
            "quick": (0.5, 1.0),      # 500ms-1s for quick actions
            "normal": (1.0, 2.0),     # 1-2s for normal actions  
            "slow": (2.0, 3.5),       # 2-3.5s for slow actions
            "typing": (0.1, 0.3),     # 100-300ms between characters
            "mouse": (0.3, 0.8),      # 300-800ms for mouse movements
            "form": (0.8, 1.5)        # 800ms-1.5s for form interactions
        }
        min_delay, max_delay = delays.get(delay_type, delays["normal"])
        await asyncio.sleep(random.uniform(min_delay, max_delay))

    async def human_type(self, element, text: str):
        """SERVER-OPTIMIZED typing - Character-by-character with proper delays"""
        await element.clear()
        await self.human_delay("quick")  # Small delay after clearing
        
        # Type character by character for more human-like behavior
        for char in text:
            await element.type(char)
            await self.human_delay("typing")  # Small delay between characters

    async def human_click_with_movement(self, page, element):
        """Click element with human-like mouse movement + FORCE CLICK modal bypass"""
        # Get element position
        box = await element.bounding_box()
        if box:
            # Add slight randomness to click position
            x = box['x'] + box['width'] / 2 + random.uniform(-5, 5)
            y = box['y'] + box['height'] / 2 + random.uniform(-5, 5)

            # Move mouse to element with delay
            await page.mouse.move(x, y)
            await self.human_delay("mouse")

            # Click with slight delay and FORCE CLICK capability
            try:
                await page.mouse.click(x, y)
            except Exception as e:
                if "intercepts pointer events" in str(e):
                    print(f"    🚫 Modal intercept detected, using FORCE click...")
                    # NUCLEAR MODAL DESTRUCTION - destroy all pointer-intercepting elements
                    await page.evaluate("""
                        // Destroy all modal-like elements that could intercept clicks
                        document.querySelectorAll('*').forEach(el => {
                            const styles = window.getComputedStyle(el);
                            if (styles.pointerEvents === 'none' ||
                                styles.position === 'fixed' ||
                                parseInt(styles.zIndex) > 1000) {
                                el.style.display = 'none !important';
                                el.style.pointerEvents = 'none !important';
                                try { el.remove(); } catch(e) {}
                            }
                        });
                    """)
                    # Use force click to bypass modal interception
                    await element.click(force=True, timeout=2000)
                else:
                    # For other errors, try force click as fallback
                    print(f"    ⚠️ Click failed ({str(e)[:50]}...), trying force click...")
                    await element.click(force=True, timeout=2000)
        else:
            # Fallback to regular click with force option
            try:
                await element.click()
            except Exception as e:
                if "intercepts pointer events" in str(e):
                    print(f"    🚫 Modal intercept detected, using FORCE click...")
                    # NUCLEAR MODAL DESTRUCTION - destroy all pointer-intercepting elements
                    await page.evaluate("""
                        // Destroy all modal-like elements that could intercept clicks
                        document.querySelectorAll('*').forEach(el => {
                            const styles = window.getComputedStyle(el);
                            if (styles.pointerEvents === 'none' ||
                                styles.position === 'fixed' ||
                                parseInt(styles.zIndex) > 1000) {
                                el.style.display = 'none !important';
                                el.style.pointerEvents = 'none !important';
                                try { el.remove(); } catch(e) {}
                            }
                        });
                    """)
                    await element.click(force=True, timeout=2000)
                else:
                    print(f"    ⚠️ Click failed, trying force click...")
                    await element.click(force=True, timeout=2000)

        await self.human_delay("quick")

    def normalize_address(self, address: str) -> str:
        """Enhanced address normalization with comprehensive ordinal and special character handling"""
        if not address:
            return ""

        # Convert to uppercase and remove extra spaces
        normalized = re.sub(r'\s+', ' ', address.upper().strip())

        # ENHANCEMENT #1: Aggressive special character normalization
        # Remove hyphens, periods, and standardize spacing
        normalized = re.sub(r'[-.\s]+', ' ', normalized).strip()

        # ENHANCEMENT #1.5: Handle apartment/unit numbers (# 10, APT 5, UNIT 2B, etc.)
        # Remove common apartment indicators to focus on core address
        apt_patterns = [
            r'\s*#\s*\d+[A-Z]*',  # "# 10", "#10A"
            r'\s*APT\s*\d+[A-Z]*',  # "APT 5", "APT 2B"
            r'\s*UNIT\s*\d+[A-Z]*',  # "UNIT 3", "UNIT 1A"
            r'\s*STE\s*\d+[A-Z]*',  # "STE 100"
            r'\s*SUITE\s*\d+[A-Z]*',  # "SUITE 200"
        ]
        for pattern in apt_patterns:
            normalized = re.sub(pattern, '', normalized)

        # ENHANCEMENT #1.6: Remove city/state suffixes for better matching
        # Common Florida cities that might appear in ZabaSearch partial addresses
        city_state_patterns = [
            r',\s*[A-Z\s]+,\s*FL\s*$',  # ", HALLANDALE BEACH, FL"
            r',\s*FL\s*$',  # ", FL"
            r'\s+FL\s*$',  # " FL"
        ]
        for pattern in city_state_patterns:
            normalized = re.sub(pattern, '', normalized)

        # Clean up any trailing commas or spaces
        normalized = re.sub(r'[,\s]+$', '', normalized).strip()

        # ENHANCEMENT #2: Comprehensive ordinal number mappings
        ordinal_mappings = {
            # Basic ordinals
            '1ST': 'FIRST', 'FIRST': '1ST',
            '2ND': 'SECOND', 'SECOND': '2ND',
            '3RD': 'THIRD', 'THIRD': '3RD',
            '4TH': 'FOURTH', 'FOURTH': '4TH',
            '5TH': 'FIFTH', 'FIFTH': '5TH',
            '6TH': 'SIXTH', 'SIXTH': '6TH',
            '7TH': 'SEVENTH', 'SEVENTH': '7TH',
            '8TH': 'EIGHTH', 'EIGHTH': '8TH',
            '9TH': 'NINTH', 'NINTH': '9TH',
            '10TH': 'TENTH', 'TENTH': '10TH',
            # Teen ordinals (special cases)
            '11TH': 'ELEVENTH', 'ELEVENTH': '11TH',
            '12TH': 'TWELFTH', 'TWELFTH': '12TH',
            '13TH': 'THIRTEENTH', 'THIRTEENTH': '13TH',
            # Twenty series
            '20TH': 'TWENTIETH', 'TWENTIETH': '20TH',
            '21ST': 'TWENTY-FIRST', 'TWENTY-FIRST': '21ST',
            '22ND': 'TWENTY-SECOND', 'TWENTY-SECOND': '22ND',
            '23RD': 'TWENTY-THIRD', 'TWENTY-THIRD': '23RD',
            # Common higher ordinals
            '30TH': 'THIRTIETH', 'THIRTIETH': '30TH',
            '40TH': 'FORTIETH', 'FORTIETH': '40TH',
            '50TH': 'FIFTIETH', 'FIFTIETH': '50TH'
        }

        # Apply ordinal mappings
        for ordinal, word in ordinal_mappings.items():
            normalized = normalized.replace(f' {ordinal} ', f' {word} ')

        # ENHANCEMENT #3: Comprehensive directional mappings
        direction_mappings = {
            ' E ': ' EAST ', ' EAST ': ' E ',
            ' W ': ' WEST ', ' WEST ': ' W ',
            ' N ': ' NORTH ', ' NORTH ': ' N ',
            ' S ': ' SOUTH ', ' SOUTH ': ' S ',
            ' NE ': ' NORTHEAST ', ' NORTHEAST ': ' NE ',
            ' NW ': ' NORTHWEST ', ' NORTHWEST ': ' NW ',
            ' SE ': ' SOUTHEAST ', ' SOUTHEAST ': ' SE ',
            ' SW ': ' SOUTHWEST ', ' SOUTHWEST ': ' SW '
        }

        # Apply directional mappings
        for short, long in direction_mappings.items():
            normalized = normalized.replace(short, long)

        # Common street type normalizations
        street_type_replacements = {
            ' STREET': ' ST',
            ' AVENUE': ' AVE',
            ' DRIVE': ' DR',
            ' COURT': ' CT',
            ' PLACE': ' PL',
            ' ROAD': ' RD',
            ' CIRCLE': ' CIR',
            ' WAY': ' WAY',
            ' BOULEVARD': ' BLVD',
            ' LANE': ' LN',
            ' TERRACE': ' TER',
            ' PARKWAY': ' PKWY',
            ' HIGHWAY': ' HWY'
        }

        for old, new in street_type_replacements.items():
            normalized = normalized.replace(old, new)

        return normalized

    def addresses_match(self, csv_address: str, zaba_address: str) -> dict:
        """Enhanced address matching with confidence scoring and improved logic"""
        if not csv_address or not zaba_address:
            return {'match': False, 'confidence': 0, 'reason': 'Missing address data'}

        csv_norm = self.normalize_address(csv_address)
        zaba_norm = self.normalize_address(zaba_address)

        print(f"    🔍 Comparing: '{csv_norm}' vs '{zaba_norm}'")

        # Extract components for flexible matching
        csv_parts = csv_norm.split()
        zaba_parts = zaba_norm.split()

        if len(csv_parts) < 2 or len(zaba_parts) < 2:
            return {'match': False, 'confidence': 0, 'reason': 'Insufficient address components'}

        # ENHANCEMENT #4: Street number must match (critical requirement)
        if csv_parts[0] != zaba_parts[0]:
            return {'match': False, 'confidence': 0, 'reason': f'Street number mismatch: {csv_parts[0]} vs {zaba_parts[0]}'}

        # ENHANCEMENT #5: Advanced token matching with variations
        def create_comprehensive_variations(parts):
            """Create comprehensive variations for better matching"""
            variations = set()

            for part in parts:
                variations.add(part)

                # Handle ordinal numbers comprehensively
                if re.match(r'^\d+$', part):
                    num = int(part)
                    ordinal_suffixes = {
                        1: ['ST', 'FIRST'], 2: ['ND', 'SECOND'], 3: ['RD', 'THIRD'],
                        4: ['TH', 'FOURTH'], 5: ['TH', 'FIFTH'], 6: ['TH', 'SIXTH'],
                        7: ['TH', 'SEVENTH'], 8: ['TH', 'EIGHTH'], 9: ['TH', 'NINTH'],
                        10: ['TH', 'TENTH'], 11: ['TH', 'ELEVENTH'], 12: ['TH', 'TWELFTH'],
                        13: ['TH', 'THIRTEENTH'], 20: ['TH', 'TWENTIETH'], 21: ['ST', 'TWENTY-FIRST'],
                        22: ['ND', 'TWENTY-SECOND'], 23: ['RD', 'TWENTY-THIRD'], 30: ['TH', 'THIRTIETH']
                    }

                    if num in ordinal_suffixes:
                        for suffix in ordinal_suffixes[num]:
                            if suffix.endswith('TH') or suffix.endswith('ST') or suffix.endswith('ND') or suffix.endswith('RD'):
                                variations.add(f"{part}{suffix}")
                            else:
                                variations.add(suffix)
                    elif num % 10 == 1 and num not in [11]:
                        variations.update([f"{part}ST", "FIRST" if num == 1 else f"TWENTY-FIRST" if num == 21 else f"{part}ST"])
                    elif num % 10 == 2 and num not in [12]:
                        variations.update([f"{part}ND", "SECOND" if num == 2 else f"TWENTY-SECOND" if num == 22 else f"{part}ND"])
                    elif num % 10 == 3 and num not in [13]:
                        variations.update([f"{part}RD", "THIRD" if num == 3 else f"TWENTY-THIRD" if num == 23 else f"{part}RD"])
                    else:
                        variations.add(f"{part}TH")

                # Handle existing ordinal suffixes
                elif re.match(r'^\d+(ST|ND|RD|TH)$', part):
                    base_num = re.sub(r'(ST|ND|RD|TH)$', '', part)
                    variations.add(base_num)
                    # Add word form variations
                    num = int(base_num)
                    word_forms = {
                        1: 'FIRST', 2: 'SECOND', 3: 'THIRD', 4: 'FOURTH', 5: 'FIFTH',
                        21: 'TWENTY-FIRST', 22: 'TWENTY-SECOND', 23: 'TWENTY-THIRD'
                    }
                    if num in word_forms:
                        variations.add(word_forms[num])

                # Handle word-form ordinals
                elif part in ['FIRST', 'SECOND', 'THIRD', 'FOURTH', 'FIFTH', 'SIXTH', 'SEVENTH', 'EIGHTH', 'NINTH', 'TENTH',
                             'ELEVENTH', 'TWELFTH', 'THIRTEENTH', 'TWENTIETH', 'TWENTY-FIRST', 'TWENTY-SECOND', 'TWENTY-THIRD']:
                    word_to_num = {
                        'FIRST': '1', 'SECOND': '2', 'THIRD': '3', 'FOURTH': '4', 'FIFTH': '5',
                        'TWENTY-FIRST': '21', 'TWENTY-SECOND': '22', 'TWENTY-THIRD': '23'
                    }
                    if part in word_to_num:
                        variations.add(word_to_num[part])
                        variations.add(f"{word_to_num[part]}ST" if part.endswith('FIRST') else
                                     f"{word_to_num[part]}ND" if part.endswith('SECOND') else
                                     f"{word_to_num[part]}RD" if part.endswith('THIRD') else
                                     f"{word_to_num[part]}TH")

                # Handle directional abbreviations
                direction_vars = {
                    'E': ['EAST'], 'EAST': ['E'], 'W': ['WEST'], 'WEST': ['W'],
                    'N': ['NORTH'], 'NORTH': ['N'], 'S': ['SOUTH'], 'SOUTH': ['S'],
                    'NE': ['NORTHEAST'], 'NORTHEAST': ['NE'], 'NW': ['NORTHWEST'], 'NORTHWEST': ['NW'],
                    'SE': ['SOUTHEAST'], 'SOUTHEAST': ['SE'], 'SW': ['SOUTHWEST'], 'SOUTHWEST': ['SW']
                }
                if part in direction_vars:
                    variations.update(direction_vars[part])

            return list(variations)

        # Get key street parts (exclude street number)
        csv_street_parts = csv_parts[1:]
        zaba_street_parts = zaba_parts[1:]

        # Create variations for both addresses
        csv_variations = create_comprehensive_variations(csv_street_parts)
        zaba_variations = create_comprehensive_variations(zaba_street_parts)

        # ENHANCEMENT #6: Advanced scoring system
        matches = 0
        matched_parts = []
        
        # SMART TOKEN CALCULATION: Use the shorter address for threshold calculation
        # This handles cases where ZabaSearch provides partial info (no city/state)
        csv_token_count = len(csv_street_parts)
        zaba_token_count = len(zaba_street_parts)
        
        # Use minimum for threshold (more lenient when one address is partial)
        # Use maximum for confidence calculation (considers all available info)
        threshold_tokens = min(csv_token_count, zaba_token_count)
        total_tokens = max(csv_token_count, zaba_token_count)

        for csv_var in csv_variations:
            if csv_var in zaba_variations and csv_var not in matched_parts:
                matches += 1
                matched_parts.append(csv_var)

        # Calculate meaningful matches early for use in multiple places
        generic_types = ['ST', 'AVE', 'DR', 'CT', 'PL', 'RD', 'LN', 'CIR', 'BLVD', 'TER', 'WAY']
        meaningful_matches = [part for part in matched_parts if part not in generic_types]

        # ENHANCEMENT #7: Smart threshold system based on address complexity and content
        
        # Use the shorter address length for determining match requirements
        # This handles partial addresses from ZabaSearch gracefully
        if threshold_tokens <= 2:
            # Simple addresses (e.g., "123 MAIN ST") - require 1 meaningful match
            required_matches = 1
        elif threshold_tokens <= 3:
            # Medium addresses (e.g., "123 MAIN ST E") - require 1-2 matches
            required_matches = 1
        else:
            # Complex addresses - require 2+ matches
            required_matches = 2

        # Calculate confidence score and match decision
        is_match = False
        reason = ""
        confidence = 0
        
        if matches >= required_matches:
            # Check for generic-only matches (potential false positives)
            if (total_tokens <= 2 and len(matched_parts) == 1 and 
                matched_parts[0] in generic_types):
                # Only generic street type matched - very low confidence
                confidence = 30
                is_match = False
                reason = f'Only generic street type "{matched_parts[0]}" matched - insufficient for match'
            else:
                # Valid match - calculate confidence
                # Base confidence from match ratio
                match_ratio = matches / total_tokens
                confidence = min(100, int(match_ratio * 100))

                # Bonus for street number match (already verified above)
                confidence += 20

                # Bonus for multiple matches
                if matches >= 2:
                    confidence += 10

                # Special bonus for meaningful street name matches (not just generic types)
                if meaningful_matches:
                    confidence += 15  # Bonus for meaningful street names like "LOMBARDY"

                # ENHANCEMENT: Boost confidence for strong core address matches
                # If we have street number + street name match, it's very likely correct
                if matches >= 1 and meaningful_matches:
                    confidence += 20  # Extra boost for strong core matches

                # Ensure minimum confidence for valid matches
                confidence = max(confidence, 75)  # Raised from 70 to 75
                
                # Cap at 100%
                confidence = min(confidence, 100)

                is_match = True
                reason = f'Found {matches}/{required_matches} required matches with meaningful street name'
        else:
            # ENHANCEMENT: More lenient for very close matches
            if matches == required_matches - 1 and meaningful_matches:
                # Almost matched - give it a chance with lower confidence
                confidence = 65
                is_match = True
                reason = f'Close match: {matches}/{required_matches} with meaningful street name (apartment variation likely)'
            else:
                is_match = False
                confidence = int((matches / required_matches) * 50)  # Partial confidence for partial matches
                reason = f'Only {matches}/{required_matches} required matches'

        result = {
            'match': is_match,
            'confidence': confidence,
            'matched_tokens': matched_parts,
            'total_tokens': total_tokens,
            'required_matches': required_matches,
            'actual_matches': matches,
            'reason': reason
        }

        print(f"    📊 Enhanced analysis: {matches} matches, {confidence}% confidence, result: {'✅' if is_match else '❌'}")
        print(f"    🔍 Matched tokens: {matched_parts}")

        return result

    async def detect_blocking(self, page) -> bool:
        """Detect if we're being blocked by ZabaSearch"""
        try:
            # Check for common blocking indicators
            page_content = await page.content()
            page_title = await page.title()

            blocking_indicators = [
                'captcha',
                'unusual traffic',
                'blocked',
                'service unavailable',
                'access denied',
                'too many requests'
            ]

            content_lower = page_content.lower()
            title_lower = page_title.lower()

            for indicator in blocking_indicators:
                if indicator in content_lower or indicator in title_lower:
                    return True

            # Don't assume no person cards = blocking
            # Sometimes ZabaSearch legitimately has no results for a person
            return False
        except:
            return False

    # Cloudflare detection removed - not needed for this environment

    # Cloudflare handling removed - not needed for this environment

    async def detect_and_handle_popups(self, page):
        """Detect and handle any popups that might appear - ENHANCED"""
        try:
            # FIRST: Handle privacy/cookie consent modal (I AGREE button)
            privacy_handled = False

            try:
                # Look for "I AGREE" button first
                agree_button = await page.wait_for_selector('text="I AGREE"', timeout=self.agreement_timeout)  # Use full agreement timeout
                if agree_button:
                    print(f"    🚨 PRIVACY MODAL DETECTED - clicking I AGREE")
                    await agree_button.click()
                    await asyncio.sleep(0.5)  # Reduced from 1
                    print(f"    ✅ Privacy modal closed with I AGREE")
                    privacy_handled = True
            except:
                # Check for other privacy modal patterns
                privacy_selectors = [
                    '[role="dialog"]',
                    '.modal',
                    '[aria-modal="true"]',
                    '.modal-container',
                    '.cky-modal',
                    '.privacy-modal'
                ]

                for selector in privacy_selectors:
                    try:
                        modal = await page.wait_for_selector(selector, timeout=self.selector_timeout)  # Use full selector timeout for reliability
                        if modal:
                            print(f"    🚨 PRIVACY MODAL DETECTED: {selector}")

                            # Try to find and click close buttons
                            close_selectors = [
                                'text="I AGREE"',
                                'text="Accept All"',
                                'text="Accept"',
                                'text="Close"',
                                'text="X"',
                                '.close-button',
                                '[aria-label="Close"]'
                            ]

                            modal_closed = False
                            for close_selector in close_selectors:
                                try:
                                    close_btn = await modal.query_selector(close_selector)
                                    if close_btn:
                                        await close_btn.click()
                                        print(f"    ✅ PRIVACY MODAL CLOSED: {close_selector}")
                                        privacy_handled = True
                                        modal_closed = True
                                        break
                                except:
                                    continue

                            if modal_closed:
                                await asyncio.sleep(0.5)  # Reduced from 1
                                break

                    except:
                        continue

            # SECOND: Continue with normal processing
            if privacy_handled:
                print(f"    ⏳ Waiting for page to settle after privacy modal...")
                await asyncio.sleep(1)  # Reduced from 2

            # Cloudflare protection removed - direct processing

            if not privacy_handled:
                pass  # No need for success message

        except Exception as e:
            print(f"    ⚠️ Popup scan error: {e}")
            print(f"    🔄 Continuing despite popup scan error...")
            # Don't fail the whole process for popup detection
            pass

    async def validate_page_ready(self, page):
        """Validate page is ready for interaction with form elements"""
        try:
            print(f"    🔍 Validating page readiness...")

            # Wait for form elements to be present
            form_selectors = [
                'input[placeholder*="First"]',
                'input[placeholder*="Last"]',
                'input[name="firstname"]',
                'input[name="lastname"]',
                '.search-form'
            ]

            for selector in form_selectors:
                try:
                    element = await page.wait_for_selector(selector, timeout=self.selector_timeout)  # Use environment timeout
                    if element:
                        print(f"    ✅ Form element found: {selector}")
                        return True
                except:
                    continue

            print(f"    ⚠️ No form elements detected")
            return False

        except Exception as e:
            print(f"    ❌ Page validation error: {e}")
            return False

    async def wait_for_results_loading(self, page):
        """Wait for results to load or no-results indicator"""
        try:
            print(f"    ⏳ Waiting for results to load...")

            # Wait for either results OR no results indicator
            result_selectors = [
                '.person-card',
                '.search-result',
                'text="No results found"',
                'text="No people found"',
                '.no-results',
                '.empty-results'
            ]

            for selector in result_selectors:
                try:
                    element = await page.wait_for_selector(selector, timeout=self.selector_timeout)  # Use environment timeout
                    if element:
                        print(f"    ✅ Results loaded: {selector}")
                        # Additional wait for network stability
                        await page.wait_for_load_state('networkidle', timeout=5000)
                        return True
                except:
                    continue

            print(f"    ⚠️ Results loading timeout")
            return False

        except Exception as e:
            print(f"    ❌ Results loading error: {e}")
            return False

    async def validate_browser_session(self, page):
        """Test browser functionality before each session"""
        try:
            print(f"    🔧 Validating browser session...")

            # Test basic page functionality
            await page.evaluate('() => console.log("Browser test")')

            # Test page navigation state
            if page.url == 'about:blank':
                print(f"    ⚠️ Browser on blank page")
                return False

            print(f"    ✅ Browser session healthy")
            return True

        except Exception as e:
            print(f"    ❌ Browser validation error: {e}")
            return False

    async def accept_terms_if_needed(self, page):
        """Accept terms and conditions if not already done"""
        if self.terms_accepted:
            return

        try:
            # Look for "I AGREE" button
            agree_button = await page.wait_for_selector('text="I AGREE"', timeout=self.agreement_timeout)  # Use full agreement timeout
            if agree_button:
                await agree_button.click()
                self.terms_accepted = True
                await self.human_delay("quick")
                print("  ✓ Accepted terms and conditions")
        except:
            # Terms already accepted or not present
            self.terms_accepted = True

    def normalize_state(self, state: str) -> str:
        """Normalize state abbreviations to full state names for ZabaSearch dropdown"""
        if not state:
            return "Florida"  # Default fallback
            
        state = state.strip().upper()
        
        # State abbreviation to full name mapping
        state_mapping = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
            'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
            'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
            'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
            'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
            'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District Of Columbia'
        }
        
        # Return mapped state or original if it's already a full name
        return state_mapping.get(state, state.title())

    async def search_person(self, page, first_name: str, last_name: str, target_address: str = "", city: str = "", state: str = "Florida") -> Optional[Dict]:
        """Search for a person on ZabaSearch with optimized processing"""
        try:
            print(f"🔍 Searching ZabaSearch: {first_name} {last_name}")
            print(f"  🌐 Navigating to ZabaSearch...")
            print(f"  🔧 DEBUG: About to navigate to https://www.zabasearch.com")

            # PRIORITY 2 FIX: Use configuration timeout instead of hardcoded value
            # CRITICAL FIX: Changed wait_until to 'load' instead of 'domcontentloaded'
            # because ZabaSearch has heavy resources that prevent domcontentloaded from firing
            await page.goto('https://www.zabasearch.com', wait_until='load', timeout=self.navigation_timeout)
            print(f"  ✅ Page loaded successfully")
            print(f"  🔧 DEBUG: Navigation completed, page URL: {page.url}")
            await asyncio.sleep(0.5)  # Reverted to original

            # Cloudflare protection removed - proceeding directly

            # Check for any other popups
            # Accept terms if needed
            await self.accept_terms_if_needed(page)

            # Fill search form using the correct selectors from Playwright MCP testing
            print(f"  🔍 Locating search form elements...")
            await self.human_delay("form")

            # Fill name fields with small timeout increases for form inputs
            print(f"  ✏️ Filling first name: {first_name}")
            # FIXED: Use environment-based selector timeout instead of hardcoded 3250ms
            first_name_box = page.get_by_role("textbox", name="eg. John")
            await first_name_box.wait_for(state="visible", timeout=self.selector_timeout)  # Uses 15s for server compatibility
            await self.human_click_with_movement(page, first_name_box)
            await self.human_type(first_name_box, first_name)
            await self.human_delay("form")

            print(f"  ✏️ Filling last name: {last_name}")
            last_name_box = page.get_by_role("textbox", name="eg. Smith")
            await last_name_box.wait_for(state="visible", timeout=self.selector_timeout)  # Uses 15s for server compatibility
            await self.human_click_with_movement(page, last_name_box)
            await self.human_type(last_name_box, last_name)
            await self.human_delay("form")

            # Fill city and state if provided
            if city:
                print(f"  🏙️ Filling city: {city}")
                try:
                    city_box = page.get_by_role("textbox", name="eg. Chicago")
                    await city_box.wait_for(state="visible", timeout=self.selector_timeout)  # Uses 15s for server compatibility
                    await self.human_click_with_movement(page, city_box)
                    await self.human_type(city_box, city)
                    await self.human_delay("form")
                    print(f"    ✅ Successfully filled city: {city}")
                except Exception as e:
                    print(f"    ⚠️ Could not fill city field: {e}")

            # Normalize state name for dropdown selection
            normalized_state = self.normalize_state(state)
            
            if normalized_state and normalized_state.upper() in ["FLORIDA", "FL"]:
                print(f"  🗺️ Selecting state: Florida")
                try:
                    state_dropdown = page.get_by_role("combobox")
                    await self.human_click_with_movement(page, state_dropdown)
                    await self.human_delay("mouse")
                    await state_dropdown.select_option("Florida")
                    await self.human_delay("form")
                    print(f"    ✅ Selected Florida")
                except Exception as e:
                    print(f"    ⚠️ Could not select Florida: {e}")
            elif normalized_state:
                print(f"  🗺️ Attempting to select state: {normalized_state}")
                try:
                    state_dropdown = page.get_by_role("combobox")
                    await self.human_click_with_movement(page, state_dropdown)
                    await self.human_delay("mouse")
                    # Try to select the normalized state name
                    await state_dropdown.select_option(normalized_state)
                    await self.human_delay("form")
                    print(f"    ✅ Selected {normalized_state}")
                except Exception as e:
                    print(f"    ⚠️ Could not select state {normalized_state}: {e}")
                    # Fallback to Florida if state selection fails
                    try:
                        await state_dropdown.select_option("Florida")
                        print(f"    🔄 Fallback: Selected Florida")
                    except Exception as fallback_error:
                        print(f"    ❌ State selection completely failed: {fallback_error}")

            await self.human_delay("slow")  # Longer pause before submitting

            # Submit search using Enter key like in test script
            print(f"  🚀 Submitting search...")
            await self.human_click_with_movement(page, last_name_box)
            await last_name_box.press("Enter")
            print(f"  ⏳ Waiting for results to load...")
            await self.human_delay("slow")  # Longer wait for results

            # Cloudflare protection removed - proceeding to data extraction

            # Try to extract data directly
            print(f"  📊 Attempting to extract person data...")
            result = await self.extract_person_data(page, first_name, last_name, target_address)

            if result:
                print(f"  ✅ Successfully extracted data for {first_name} {last_name}")
            else:
                print(f"  ❌ No matching data found for {first_name} {last_name}")

            return result

        except Exception as e:
            error_msg = str(e).lower()
            print(f"  ❌ Search error: {e}")
            print(f"  🔍 Error type: {type(e).__name__}")

            # Enhanced error classification for better recovery
            error_category = "unknown"

            if any(term in error_msg for term in ['connection', 'socket', 'timeout', 'closed']):
                error_category = "network"
                print(f"  🌐 Network/Connection error detected")
            elif any(term in error_msg for term in ['browser', 'page', 'context']):
                error_category = "browser"
                print(f"  🔧 Browser/Page error detected")
            elif any(term in error_msg for term in ['selector', 'element', 'query']):
                error_category = "selector"
                print(f"  🎯 Selector/Element error detected")
            elif 'cloudflare' in error_msg:
                error_category = "cloudflare"
                print(f"  🛡️ Cloudflare protection detected")
            else:
                print(f"  ❓ Unclassified error: {error_msg}")

            print(f"  � Search failed - Error category: {error_category}")
            return None

        finally:
            # ALWAYS print bandwidth stats - even if search failed
            self.print_bandwidth_stats()

    async def extract_person_data(self, page, target_first_name: str, target_last_name: str, target_address: str = "") -> Optional[Dict]:
        """Extract person data from ZabaSearch results page"""
        try:
            print("  📋 Extracting person data...")

            # Get all person result containers using the class I found in debug
            person_cards = await page.query_selector_all('.person')

            if not person_cards:
                print("  ❌ No person cards found")
                return None

            print(f"  ✅ Found {len(person_cards)} person cards")

            for i, card in enumerate(person_cards):
                print(f"  🔍 Checking result #{i+1}")

                # Get the card text to check if it's the right person
                try:
                    card_text = await card.inner_text()

                    # Check if this card contains our target person
                    if target_first_name.lower() not in card_text.lower() or target_last_name.lower() not in card_text.lower():
                        continue

                    print(f"  ✅ Found matching person in card #{i+1}")

                    # Extract addresses from this card
                    person_addresses = []

                    # Look for address patterns in the text
                    address_lines = card_text.split('\n')
                    for line in address_lines:
                        line = line.strip()
                        # Look for lines that look like addresses - more flexible pattern
                        # Pattern: starts with number, contains street words, may have city/state
                        if (re.search(r'^\d+\s+', line) and  # starts with number
                            re.search(r'(?:ST|DR|AVE|CT|RD|LN|BLVD|WAY|PL|CIR|TER|DRIVE|STREET|AVENUE|COURT|ROAD|LANE|BOULEVARD|PLACE|CIRCLE|TERRACE)', line.upper()) and
                            len(line) > 10):  # reasonable length
                            person_addresses.append(line)
                        # Also look for city, state zip patterns
                        elif re.search(r'[A-Za-z\s]+,\s*[A-Z]{2}\s+\d{5}', line):
                            person_addresses.append(line)

                    print(f"    📍 Found {len(person_addresses)} addresses in this card")

                    # Check if any address matches our target
                    address_match = False
                    address_match_info = None
                    if target_address:
                        for addr in person_addresses:
                            match_result = self.addresses_match(target_address, addr)
                            if match_result['match']:
                                address_match = True
                                address_match_info = match_result
                                print(f"    ✅ Address match found: {addr} (Confidence: {match_result['confidence']}%)")
                                break
                    else:
                        address_match = True  # If no target address, accept any result

                    if not address_match:
                        print(f"    ❌ No address match for result #{i+1}")
                        continue

                    # Extract phone numbers with enhanced fallback strategies
                    phones = {"primary": None, "secondary": None, "all": []}

                    try:
                        # Strategy 1: Look specifically for "Last Known Phone Numbers" section with multiple selectors
                        phone_section_selectors = [
                            'h3:has-text("Last Known Phone Numbers")',
                            'text="Last Known Phone Numbers"',
                            'text="Phone Numbers"',
                            'text="Phone"',
                            '.phone-section',
                            '[data-phone]'
                        ]

                        last_known_section = None
                        for selector in phone_section_selectors:
                            try:
                                last_known_section = await card.query_selector(selector)
                                if last_known_section:
                                    print(f"    ✅ Found phone section with: {selector}")
                                    break
                            except:
                                continue

                        if last_known_section:
                            print("    🎯 Found 'Last Known Phone Numbers' section")

                            # Get next sibling elements that contain the phone numbers
                            phone_content_elements = await card.query_selector_all('h3:has-text("Last Known Phone Numbers") ~ *')

                            section_text = ""
                            for element in phone_content_elements:
                                try:
                                    element_text = await element.inner_text()
                                    section_text += element_text + "\n"
                                    # Stop if we hit another section heading
                                    if any(heading in element_text for heading in ["Associated Email", "Associated Phone", "Jobs", "Past Addresses"]):
                                        break
                                except:
                                    continue

                            if section_text.strip():
                                print(f"    📋 Section text: {section_text[:200]}...")

                                # Extract phone numbers only from this specific section
                                phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                                phone_matches = re.findall(phone_pattern, section_text)

                                if phone_matches:
                                    # Clean up phone numbers
                                    cleaned_phones = []
                                    for phone in phone_matches:
                                        # Standardize format to (XXX) XXX-XXXX
                                        digits = re.sub(r'\D', '', phone)
                                        if len(digits) == 10:
                                            formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                            if formatted not in cleaned_phones:
                                                cleaned_phones.append(formatted)

                                    phones["all"] = cleaned_phones

                                    # Look for primary phone designation in the section text
                                    primary_found = False
                                    for phone in cleaned_phones:
                                        # Check if this phone has "(Primary Phone)" designation
                                        if "Primary Phone" in section_text and phone in section_text:
                                            # Find the line containing this phone number
                                            lines = section_text.split('\n')
                                            for line in lines:
                                                if phone.replace('(', '').replace(')', '').replace('-', '').replace(' ', '') in line.replace('(', '').replace(')', '').replace('-', '').replace(' ', ''):
                                                    if "Primary Phone" in line or "primary" in line.lower():
                                                        phones["primary"] = phone
                                                        primary_found = True
                                                        print(f"    👑 Found designated primary phone: {phone}")
                                                        break
                                            if primary_found:
                                                break

                                    # If no explicit primary found, use first phone as primary
                                    if not primary_found and cleaned_phones:
                                        phones["primary"] = cleaned_phones[0]
                                        print(f"    📞 Using first phone as primary: {cleaned_phones[0]}")

                                    # Set secondary phone
                                    if len(cleaned_phones) > 1:
                                        for phone in cleaned_phones:
                                            if phone != phones["primary"]:
                                                phones["secondary"] = phone
                                                break

                                    print(f"    📞 Found {len(cleaned_phones)} phone numbers from 'Last Known Phone Numbers' section")
                                    for phone in cleaned_phones:
                                        print(f"      📞 {phone}")
                        else:
                            print("    ⚠️ 'Last Known Phone Numbers' section not found, trying broader search...")

                            # Fallback: look for phone numbers in the entire card but with more specific filtering
                            phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                            phone_matches = re.findall(phone_pattern, card_text)

                            if phone_matches:
                                # Clean up phone numbers
                                cleaned_phones = []
                                for phone in phone_matches:
                                    # Standardize format to (XXX) XXX-XXXX
                                    digits = re.sub(r'\D', '', phone)
                                    if len(digits) == 10:
                                        formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                        if formatted not in cleaned_phones:
                                            cleaned_phones.append(formatted)

                                # Limit to first 2 phones to avoid pulling all associated numbers
                                cleaned_phones = cleaned_phones[:2]
                                phones["all"] = cleaned_phones

                                if cleaned_phones:
                                    phones["primary"] = cleaned_phones[0]
                                    if len(cleaned_phones) > 1:
                                        phones["secondary"] = cleaned_phones[1]

                                print(f"    � Fallback: Found {len(cleaned_phones)} phone numbers (limited to 2)")
                                for phone in cleaned_phones:
                                    print(f"      📞 {phone}")

                    except Exception as e:
                        print(f"    ⚠️ Error extracting phones from specific section: {e}")
                        # Ultimate fallback to original method but limited
                        phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                        phone_matches = re.findall(phone_pattern, card_text)

                        if phone_matches:
                            cleaned_phones = []
                            for phone in phone_matches[:2]:  # Limit to first 2
                                digits = re.sub(r'\D', '', phone)
                                if len(digits) == 10:
                                    formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                    if formatted not in cleaned_phones:
                                        cleaned_phones.append(formatted)

                            phones["all"] = cleaned_phones
                            if cleaned_phones:
                                phones["primary"] = cleaned_phones[0]
                                if len(cleaned_phones) > 1:
                                    phones["secondary"] = cleaned_phones[1]

                            # Fallback: look for phone numbers in the entire card but with more specific filtering
                            phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                            phone_matches = re.findall(phone_pattern, card_text)

                            if phone_matches:
                                # Clean up phone numbers
                                cleaned_phones = []
                                for phone in phone_matches:
                                    # Standardize format to (XXX) XXX-XXXX
                                    digits = re.sub(r'\D', '', phone)
                                    if len(digits) == 10:
                                        formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                        if formatted not in cleaned_phones:
                                            cleaned_phones.append(formatted)

                                # Limit to first 2 phones to avoid pulling all associated numbers
                                cleaned_phones = cleaned_phones[:2]
                                phones["all"] = cleaned_phones

                                if cleaned_phones:
                                    phones["primary"] = cleaned_phones[0]
                                    if len(cleaned_phones) > 1:
                                        phones["secondary"] = cleaned_phones[1]

                                print(f"    📞 Fallback: Found {len(cleaned_phones)} phone numbers (limited to 2)")
                                for phone in cleaned_phones:
                                    print(f"      📞 {phone}")

                    except Exception as e:
                        print(f"    ⚠️ Error extracting phones from specific section: {e}")
                        # Ultimate fallback to original method but limited
                        phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                        phone_matches = re.findall(phone_pattern, card_text)

                        if phone_matches:
                            cleaned_phones = []
                            for phone in phone_matches[:2]:  # Limit to first 2
                                digits = re.sub(r'\D', '', phone)
                                if len(digits) == 10:
                                    formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                    if formatted not in cleaned_phones:
                                        cleaned_phones.append(formatted)

                            phones["all"] = cleaned_phones
                            if cleaned_phones:
                                phones["primary"] = cleaned_phones[0]
                                if len(cleaned_phones) > 1:
                                    phones["secondary"] = cleaned_phones[1]

                    # Return the data if we found phone numbers
                    if phones["all"]:
                        return {
                            "name": f"{target_first_name} {target_last_name}",
                            "primary_phone": phones["primary"],
                            "secondary_phone": phones["secondary"],
                            "all_phones": phones["all"],
                            "matched_address": person_addresses[0] if person_addresses else "",
                            "address_match": address_match,
                            "total_phones": len(phones["all"])
                        }
                    else:
                        print(f"    ❌ No phone numbers found in result #{i+1}")
                        continue

                except Exception as e:
                    print(f"    ❌ Error processing card #{i+1}: {e}")
                    continue

            print("  ❌ No matching records with phone numbers found")
            return None

        except Exception as e:
            print(f"  ❌ Extraction error: {e}")
            return None

    async def process_csv_batch(self, csv_path: str, output_path: Optional[str] = None, start_record: int = 1, end_record: Optional[int] = None):
        """
        Process CSV batch for pipeline scheduler compatibility.

        Args:
            csv_path: Input CSV file path
            output_path: Output CSV file path (if None, saves to same file)
            start_record: Starting record number (1-based)
            end_record: Ending record number (None = all records)
        """
        print(f"📞 ZABASEARCH PHONE EXTRACTOR - PIPELINE BATCH MODE (1 record per session)")
        print("=" * 70)

        # Set output path to same as input if not provided
        if not output_path:
            output_path = csv_path

        # Load CSV
        try:
            df = pd.read_csv(csv_path)
            print(f"✓ Loaded {len(df)} records from CSV")
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return

        # Process the specified range
        total_records = len(df)
        if end_record is None:
            end_record = total_records

        # Ensure bounds are valid
        start_record = max(1, start_record)
        end_record = min(end_record, total_records)

        print(f"✓ Processing records {start_record} to {end_record} of {total_records}")

        # Extract records for processing
        records_to_process = []
        for idx in range(start_record - 1, end_record):  # Convert to 0-based indexing
            row = df.iloc[idx]

            # Process BOTH DirectName and IndirectName records
            for prefix in ['IndirectName', 'DirectName']:  # Try IndirectName first
                name_col = f"{prefix}_Cleaned"
                address_col = f"{prefix}_Address"
                city_col = f"{prefix}_City"
                state_col = f"{prefix}_State"
                type_col = f"{prefix}_Type"

                name = row.get(name_col, '')
                address = row.get(address_col, '')
                city = row.get(city_col, '') if city_col in df.columns else ''
                state = row.get(state_col, '') if state_col in df.columns else ''
                record_type = row.get(type_col, '')

                # Check if we have valid name and address for a Person (not Business/Organization)
                if (name and address and pd.notna(name) and pd.notna(address) and
                    str(name).strip() and str(address).strip() and
                    record_type == 'Person'):

                    # Check if we already have phone numbers for this record
                    phone_col = f"{prefix}_Phone_Primary"
                    if phone_col in df.columns and pd.notna(row.get(phone_col)) and str(row.get(phone_col)).strip():
                        print(f"  ⏭️ Skipping {name} - already has phone number")
                        break  # Skip to next row

                    records_to_process.append({
                        'name': str(name).strip(),
                        'address': str(address).strip(),
                        'city': str(city).strip() if city else '',
                        'state': str(state).strip() if state else 'Florida',
                        'row_index': idx,
                        'column_prefix': prefix
                    })
                    break  # Found a valid record for this row, move to next row

        print(f"✓ Found {len(records_to_process)} records to process")

        # Add new columns for phone data if they don't exist
        phone_columns = ['_Phone_Primary', '_Phone_Secondary', '_Phone_All', '_Address_Match']
        for record in records_to_process:
            prefix = record['column_prefix']
            for col in phone_columns:
                col_name = f"{prefix}{col}"
                if col_name not in df.columns:
                    df[col_name] = ''
                    df[col_name] = df[col_name].astype('object')

        # Process each record in its own browser session (one search per session)
        total_success = 0

        for record_num, record in enumerate(records_to_process, 1):
            print(f"\n{'='*80}")
            print(f"🔄 RECORD #{record_num}/{len(records_to_process)} - ONE SEARCH PER SESSION")
            print(f"{'='*80}")
            print(f"  👤 Name: {record['name']}")
            print(f"  📍 Address: {record['address']}")

            # Get proxy configuration for Cloudflare bypass (MANDATORY)
            proxy = None
            try:
                from proxy_manager import proxy_manager
                proxy = proxy_manager.get_proxy_for_zabasearch()
                if proxy:
                    print(f"🔒 Using proxy: {proxy['server']}")
                else:
                    print("❌ No proxy available - aborting batch")
                    break  # Abort entire batch if no proxy
            except ImportError:
                print("❌ Proxy manager not available - aborting batch")
                break
            except Exception as e:
                print(f"❌ Proxy error: {e} - aborting batch")
                break

            # Create new browser session for EACH record - MAXIMUM STEALTH
            async with async_playwright() as playwright:
                browser, context = await self.create_stealth_browser(playwright, browser_type='firefox', proxy=proxy)
                page = await context.new_page()
                session_success = False

                try:
                    # Parse name
                    name_parts = record['name'].split()
                    if len(name_parts) < 2:
                        print("  ❌ Invalid name format - skipping")
                        continue

                    first_name = name_parts[0]
                    last_name = name_parts[1]
                    print(f"  ✅ Parsed name: '{first_name}' '{last_name}'")

                    # Use city and state from record (with fallbacks)
                    city = record.get('city', '').strip()
                    state = record.get('state', 'Florida').strip()

                    # Fallback to defaults if empty
                    final_city = city if city else 'HALLANDALE BEACH'
                    final_state = state if state else 'Florida'

                    print(f"  🏙️ Using city: '{final_city}', state: '{final_state}'")

                    # Search ZabaSearch
                    person_data = await self.search_person(page, first_name, last_name, record['address'], final_city, final_state)

                    if person_data:
                        print(f"  🎉 SUCCESS! Found matching person with {person_data['total_phones']} phone(s)")

                        # Update CSV with phone data
                        row_idx = record['row_index']
                        prefix = record['column_prefix']

                        df.at[row_idx, f"{prefix}_Phone_Primary"] = str(person_data.get('primary_phone', ''))
                        df.at[row_idx, f"{prefix}_Phone_Secondary"] = str(person_data.get('secondary_phone', ''))
                        df.at[row_idx, f"{prefix}_Phone_All"] = ', '.join(person_data.get('all_phones', []))
                        df.at[row_idx, f"{prefix}_Address_Match"] = str(person_data.get('matched_address', ''))

                        session_success = True
                        total_success += 1

                        print(f"  📞 Primary: {person_data.get('primary_phone', 'None')}")
                        if person_data.get('secondary_phone'):
                            print(f"  📞 Secondary: {person_data.get('secondary_phone')}")
                    else:
                        print(f"  ❌ No results found for {record['name']}")

                except Exception as e:
                    print(f"  ❌ Search error: {e}")

                finally:
                    # Cleanup
                    try:
                        await page.close()
                    except:
                        pass
                    try:
                        await context.close()
                    except:
                        pass
                    try:
                        await browser.close()
                    except:
                        pass
                    gc.collect()

        # Save to output file (NOT in-place if output_path is different)
        df.to_csv(output_path, index=False)
        print(f"\n💾 BATCH PROCESSING COMPLETE!")
        print(f"📊 Successfully found phone numbers for {total_success}/{len(records_to_process)} records")
        print(f"💾 Results saved to: {output_path}")
        print(f"✅ Phone numbers added as new columns!")

    async def process_csv_with_sessions(self, csv_path: str):
        """Process CSV records with 2 records per session - saves to same file"""
        print(f"📞 ZABASEARCH PHONE EXTRACTOR - OPTIMIZED (1 record per session)")
        print("=" * 70)

        # Load CSV
        try:
            df = pd.read_csv(csv_path)
            print(f"✓ Loaded {len(df)} records from CSV")
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return

        # Find records with addresses - adapted for broward_lis_pendens CSV format
        records_with_addresses = []
        for _, row in df.iterrows():
            # Process both DirectName and IndirectName records
            for prefix in ['DirectName', 'IndirectName']:
                name_col = f"{prefix}_Cleaned"
                address_col = f"{prefix}_Address"
                city_col = f"{prefix}_City"
                state_col = f"{prefix}_State"
                type_col = f"{prefix}_Type"

                name = row.get(name_col, '')
                address = row.get(address_col, '')
                city = row.get(city_col, '')
                state = row.get(state_col, '')
                record_type = row.get(type_col, '')

                # Check if we have valid name and address for a Person (not Business/Organization)
                if (name and address and pd.notna(name) and pd.notna(address) and
                    str(name).strip() and str(address).strip() and
                    record_type == 'Person'):

                    # ENHANCED: Check Skip_ZabaSearch flag first (respects intelligent phone formatter decision)
                    skip_zabasearch = row.get('Skip_ZabaSearch', False)
                    if skip_zabasearch:
                        print(f"  ⏭️ Skipping {name} - Skip_ZabaSearch flag set (already has phone data)")
                        continue

                        # Legacy check: Also check if we already have phone numbers in DirectName/IndirectName columns
                    phone_col = f"{prefix}_Phone_Primary"
                    if phone_col in df.columns and pd.notna(row.get(phone_col)) and str(row.get(phone_col)).strip():
                        print(f"  ⏭️ Skipping {name} - already has phone number in {phone_col}")
                        continue

                    records_with_addresses.append({
                        'name': str(name).strip(),
                        'address': str(address).strip(),
                        'city': str(city).strip() if city and pd.notna(city) else '',
                        'state': str(state).strip() if state and pd.notna(state) else 'Florida',  # Default to Florida
                        'row_index': row.name,
                        'column_prefix': prefix,  # Use 'DirectName' or 'IndirectName'
                        'raw_row_data': row.to_dict()  # Store entire row for smart address processing
                    })

        print(f"✓ Found {len(records_with_addresses)} total records with person names and addresses")

        # Process all records (no skipping)
        remaining_records = records_with_addresses

        print(f"✓ Records to process: {len(remaining_records)}")
        print(f"✓ Processing 1 record per session - MAXIMUM STEALTH")

        # Add new columns for phone data with STANDARD NAMES
        phone_columns = ['_Phone_Primary', '_Phone_Secondary', '_Phone_All', '_Address_Match']

        # Check if PRIMARY/SECONDARY phone columns already exist
        has_primary_phone = any('Primary' in col and 'Phone' in col for col in df.columns)
        has_secondary_phone = any('Secondary' in col and 'Phone' in col for col in df.columns)

        print(f"📱 Phone column status:")
        print(f"  ✅ Has Primary Phone column: {has_primary_phone}")
        print(f"  ✅ Has Secondary Phone column: {has_secondary_phone}")

        # Add standard phone columns if they don't exist
        if not has_primary_phone:
            df['Primary_Phone'] = ''
            df['Primary_Phone'] = df['Primary_Phone'].astype('object')  # Ensure string type
            print(f"  ➕ Added Primary_Phone column")
        if not has_secondary_phone:
            df['Secondary_Phone'] = ''
            df['Secondary_Phone'] = df['Secondary_Phone'].astype('object')  # Ensure string type
            print(f"  ➕ Added Secondary_Phone column")

        # Also add the prefixed columns for compatibility
        for record in remaining_records:
            prefix = record['column_prefix']
            for col in phone_columns:
                col_name = f"{prefix}{col}"
                if col_name not in df.columns:
                    df[col_name] = ''
                    df[col_name] = df[col_name].astype('object')  # Ensure string type

        # Process records in sessions of 1 - ONE SEARCH PER SESSION
        session_size = 1
        total_sessions = len(remaining_records)  # Each record gets its own session
        total_success = 0

        for session_num in range(total_sessions):
            # STAGGERED BATCH START: Add delay between sessions to prevent conflicts
            if session_num > 0:  # No delay for first session
                stagger_delay = 1.5  # 1.5 seconds between each session
                print(f"⏳ Staggered delay: {stagger_delay}s before session #{session_num + 1}")
                await asyncio.sleep(stagger_delay)

            session_start = session_num * session_size
            session_end = min(session_start + session_size, len(remaining_records))
            session_records = remaining_records[session_start:session_end]

            print(f"\n{'='*80}")
            print(f"🔄 SESSION #{session_num + 1}/{total_sessions} - ONE SEARCH")
            print(f"🎯 Record {session_start + 1} of {len(remaining_records)}")
            print(f"{'='*80}")

            # Create new browser session for EACH SINGLE record - MAXIMUM STEALTH
            async with async_playwright() as playwright:

                               # Get proxy for this session with smart distribution
                proxy = None
                try:
                    from proxy_manager import proxy_manager
                    proxy = proxy_manager.get_proxy_for_zabasearch()
                    if proxy:
                        print(f"🔒 Using proxy: {proxy['server']}")
                    else:
                        print("❌ No proxy available - aborting session")
                        return  # Don't waste time without proxy
                except ImportError:
                    print("❌ Proxy manager not available - aborting session")
                    return  # Don't waste time without proxy
                except Exception as e:
                    print(f"❌ Proxy error: {e} - aborting session")
                    return  # Don't waste time without proxy

                # Create Firefox browser with proxy and bandwidth optimization
                browser, context = await self.create_stealth_browser(playwright, browser_type='firefox', proxy=proxy)
                page = await context.new_page()

                session_success = 0

                try:
                    for i, record in enumerate(session_records, 1):
                        print(f"\n{'='*60}")
                        print(f"🔍 PROCESSING SINGLE RECORD")
                        print(f"{'='*60}")
                        print(f"  👤 Name: {record['name']}")
                        print(f"  📍 Address: {record['address']}")

                        # Parse name
                        name_parts = record['name'].split()
                        if len(name_parts) < 2:
                            print("  ❌ Invalid name format - skipping")
                            continue

                        first_name = name_parts[0]
                        last_name = name_parts[1]
                        print(f"  ✅ Parsed name: '{first_name}' '{last_name}'")

                        # Extract city and state from address for better matching
                        # Always set address_str for later use
                        address_str = str(record['address']).strip()
                        print(f"  🔍 Parsing address: '{address_str}'")

                        # NEW: Use city and state directly from the separate columns
                        city = record.get('city', '').strip()
                        state = record.get('state', 'Florida').strip()

                        # If no city in separate column, fall back to default
                        if not city:
                            city = "UNKNOWN"

                        print(f"  📍 Using - City: '{city}', State: '{state}'")

                        # Use the address and location info for ZabaSearch
                        enhanced_address = record['address']
                        final_city = city if city != "UNKNOWN" else ""
                        final_state = state

                        print(f"  🔍 Searching ZabaSearch for: {first_name} {last_name}")
                        print(f"  📍 Address: {enhanced_address}")
                        print(f"  🏙️ City: {final_city}, State: {final_state}")

                        # Call ZabaSearch
                        print(f"  🚀 Starting ZabaSearch lookup with enhanced address...")
                        # Use direct city and state from formatted columns
                        final_city = record['city'] if record['city'] else 'HALLANDALE BEACH'  # Default fallback
                        final_state = record['state'] if record['state'] else 'Florida'  # Default fallback

                        print(f"  🏙️ Using city: '{final_city}', state: '{final_state}'")
                        try:
                            person_data = await self.search_person(page, first_name, last_name, enhanced_address, final_city, final_state)
                        except Exception as search_error:
                            print(f"  💥 CRITICAL ERROR during search: {search_error}")
                            print(f"  🔍 Error type: {type(search_error).__name__}")

                            # Try to continue after error
                            person_data = None

                        if not person_data:
                            # No results - leave fields empty
                            print(f"  ❌ No results found for {record['name']}")
                            continue

                        print(f"  🎉 SUCCESS! Found matching person with {person_data['total_phones']} phone(s)")

                        # Update CSV with phone data - BOTH PREFIXED AND STANDARD COLUMNS
                        row_idx = record['row_index']
                        prefix = record['column_prefix']

                        # Prefixed columns (for compatibility) - with proper dtype handling
                        primary_col = f"{prefix}_Phone_Primary"
                        secondary_col = f"{prefix}_Phone_Secondary"
                        all_col = f"{prefix}_Phone_All"
                        match_col = f"{prefix}_Address_Match"

                        # Ensure columns are string type before assignment
                        for col in [primary_col, secondary_col, all_col, match_col]:
                            if col in df.columns:
                                df[col] = df[col].astype('object')

                        # Safe assignment with string conversion
                        df.loc[row_idx, primary_col] = str(person_data.get('primary_phone', ''))
                        df.loc[row_idx, secondary_col] = str(person_data.get('secondary_phone', ''))
                        df.loc[row_idx, all_col] = str(', '.join(person_data.get('all_phones', [])))
                        df.loc[row_idx, match_col] = str(person_data.get('matched_address', ''))

                        # STANDARD COLUMNS - Primary and Secondary Phone with proper dtype handling
                        if 'Primary_Phone' in df.columns:
                            df['Primary_Phone'] = df['Primary_Phone'].astype('object')
                            df.loc[row_idx, 'Primary_Phone'] = str(person_data.get('primary_phone', ''))
                        if 'Secondary_Phone' in df.columns:
                            df['Secondary_Phone'] = df['Secondary_Phone'].astype('object')
                            df.loc[row_idx, 'Secondary_Phone'] = str(person_data.get('secondary_phone', ''))

                        session_success += 1
                        print(f"  📞 Primary: {person_data.get('primary_phone', 'None')}")
                        if person_data.get('secondary_phone'):
                            print(f"  📞 Secondary: {person_data.get('secondary_phone')}")
                        print(f"  📞 Total phones: {len(person_data.get('all_phones', []))}")
                        print(f"  🏆 SUCCESS - Session complete!")

                        # NO DELAY - session ends immediately after single search

                except Exception as e:
                    print(f"\n💥 CRITICAL SESSION ERROR: {e}")
                    print(f"🔍 Error type: {type(e).__name__}")
                    print(f"📊 Session status: {session_success} successful records before crash")       

                finally:
                    # ENHANCED BROWSER CLEANUP WITH COMPLETE SESSION TERMINATION
                    try:
                        print(f"\n🔄 STARTING SESSION CLEANUP...")

                        # Step 1: Close all pages
                        if context:
                            pages = context.pages
                            print(f"  📄 Closing {len(pages)} open pages...")
                            for page_item in pages:
                                try:
                                    await page_item.close()
                                    print(f"    ✅ Page closed")
                                except:
                                    pass

                        # Step 2: Close context (isolates sessions)
                        if context:
                            print(f"  🧬 Closing browser context (session isolation)...")
                            await context.close()
                            print(f"    ✅ Context closed - session data cleared")

                        # Step 3: Close browser process completely
                        if browser:
                            print(f"  🔧 Terminating browser process...")
                            await browser.close()
                            print(f"    ✅ Browser process terminated")

                        # Step 4: Faster cleanup delay
                        print(f"  ⏳ Waiting for complete process termination...")
                        await asyncio.sleep(1)  # Reduced from 2

                        # Step 5: Force garbage collection
                        gc.collect()
                        print(f"  🗑️ Memory cleanup completed")

                        print(f"  ✅ SESSION CLEANUP FINISHED")
                        print(f"  🛡️ All browser fingerprints cleared for next session")

                    except Exception as cleanup_error:
                        print(f"  ⚠️ Cleanup warning: {cleanup_error}")

                    # Always try to save progress after each session
                    try:
                        df.to_csv(csv_path, index=False)
                        print(f"💾 Session progress saved: {session_success} records processed in this session")
                    except:
                        pass

                # Update total success count
                total_success += session_success

                print(f"\n✅ SESSION #{session_num + 1} COMPLETE!")
                print(f"📊 Single record result: {'SUCCESS' if session_success > 0 else 'NO RESULTS'}")
                print(f"🎯 Total successful so far: {total_success}")

                # AUTO-SAVE EVERY 20 RECORDS TO PREVENT DATA LOSS
                if (session_num + 1) % 20 == 0:
                    try:
                        backup_path = csv_path.replace('.csv', f'_backup_after_{session_num + 1}_records.csv')
                        df.to_csv(backup_path, index=False)
                        print(f"💾 AUTO-SAVE: Progress backed up to {backup_path}")
                        print(f"🛡️ Protection: {session_num + 1} records processed safely")
                    except Exception as save_error:
                        print(f"⚠️ Auto-save failed: {save_error}")

                # SMART delay between sessions to avoid Cloudflare detection
                if session_num < total_sessions - 1:
                    print(f"\n⚡ Quick 0.25 second delay before next session...")
                    await asyncio.sleep(0.25)  # 20% increase: 0.2s -> 0.25s for better Cloudflare evasion

        print(f"\n🎉 ALL PROCESSING COMPLETE!")
        print(f"📊 Successfully found phone numbers for {total_success}/{len(remaining_records)} records")
        if len(remaining_records) > 0:
            percentage = (total_success/len(remaining_records)*100)
            print(f"📈 Success rate: {percentage:.1f}%")
        else:
            print(f"📈 No records to process")

        # Save final results back to the original CSV file
        df.to_csv(csv_path, index=False)
        print(f"💾 Final results saved back to: {csv_path}")
        print(f"✅ Phone numbers added as new columns in the original CSV!")


def parse_args():
    parser = argparse.ArgumentParser(description="ZabaSearch Phone Number Extractor - OPTIMIZED (1 record per session)")
    parser.add_argument('--input', type=str, help='Input CSV file (auto-detect if not specified)')
    parser.add_argument('--show-browser', action='store_true', help='Show browser GUI (default is headless mode)')
    return parser.parse_args()

async def main():
    args = parse_args()

    def find_latest_csv_with_addresses():
        """Find the latest CSV file with addresses in weekly_output folder"""
        print("🔍 Looking for CSV files with address data...")

        # Search patterns for CSV files with addresses
        search_patterns = [
            'weekly_output/*processed_with_addresses*.csv',
            'weekly_output/*_with_addresses*.csv',
            'weekly_output/broward_lis_pendens*.csv',
            'weekly_output/missing_phone_numbers*.csv',
            'weekly_output/*.csv'
        ]

        found_files = []
        for pattern in search_patterns:
            files = glob.glob(pattern)
            for file in files:
                if os.path.exists(file):
                    # Check if file has address columns
                    try:
                        df_test = pd.read_csv(file, nrows=1)
                        address_columns = [col for col in df_test.columns if 'address' in col.lower()]
                        if address_columns:
                            mod_time = os.path.getmtime(file)
                            found_files.append((file, mod_time))
                            print(f"  📄 Found: {file} (has address columns: {address_columns})")
                    except Exception as e:
                        print(f"  ⚠️ Could not read {file}: {e}")

        if not found_files:
            print("❌ No CSV files with address columns found")
            return None

        # Sort by modification time (newest first)
        found_files.sort(key=lambda x: x[1], reverse=True)
        latest_file = found_files[0][0]
        print(f"✅ Selected latest file: {latest_file}")
        return latest_file

    # Find CSV file
    csv_path = args.input if args.input else None
    # Headless by default, unless --show-browser is specified
    headless_mode = not args.show_browser
    extractor = ZabaSearchExtractor(headless=headless_mode)

    # Use existing output file if it exists, otherwise find latest CSV with addresses
    import glob, os
    if not csv_path:
        csv_path = find_latest_csv_with_addresses()
        if not csv_path:
            print(f'❌ No CSV file with addresses found!')
            return
        print(f'✅ Using auto-detected CSV file: {csv_path}')

    print(f'✅ Will save results directly to: {csv_path}')

    print(f"\n🔄 STARTING ZabaSearch extraction with SESSION-BASED processing...")
    print(f"🛡️ Enhanced with optimized processing and popup handling")
    print(f"🚀 OPTIMIZED: 1 search per session - MAXIMUM STEALTH & SPEED")
    print(f"⚡ MINIMAL delays - ULTRA FAST processing")
    print("=" * 70)

    try:
        await extractor.process_csv_with_sessions(csv_path)

    except Exception as e:
        print(f"❌ Error in processing: {e}")

    print(f"\n✅ ALL PROCESSING COMPLETE!")
    print(f"💾 Final results saved in: {csv_path}")
    print(f"✅ Phone numbers added directly to original CSV file!")

if __name__ == "__main__":
    asyncio.run(main())
