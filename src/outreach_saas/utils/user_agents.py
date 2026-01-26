"""User-Agent rotation for anti-bot protection.

Provides realistic user agents and header combinations to avoid
detection as bot traffic.

Usage:
    from user_agents import get_random_headers
    
    response = requests.get(url, headers=get_random_headers())
"""
import random
from typing import Dict

# Realistic user agents (updated 2026)
USER_AGENTS = [
    # Chrome on Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    
    # Chrome on macOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    
    # Firefox on Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    
    # Firefox on macOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:120.0) Gecko/20100101 Firefox/120.0',
    
    # Safari on macOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    
    # Edge on Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
    
    # Chrome on Linux
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

# Accept-Language variations
ACCEPT_LANGUAGES = [
    'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
    'it-IT,it;q=0.9,en;q=0.8',
    'it;q=0.9,en-US;q=0.8,en;q=0.7',
    'it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3',
]

# Referer variations (look like natural browsing)
REFERERS = [
    'https://www.google.com/',
    'https://www.google.it/',
    'https://www.bing.com/',
    'https://duckduckgo.com/',
    '',  # No referer sometimes
]


def get_random_user_agent() -> str:
    """Get a random user agent string."""
    return random.choice(USER_AGENTS)


def get_random_headers(include_referer: bool = True) -> Dict[str, str]:
    """Get a complete set of randomized headers.
    
    Args:
        include_referer: Whether to include Referer header (default True)
    
    Returns:
        Dictionary of HTTP headers
    
    Example:
        >>> headers = get_random_headers()
        >>> response = requests.get('https://example.com', headers=headers)
    """
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': random.choice(ACCEPT_LANGUAGES),
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }
    
    if include_referer:
        referer = random.choice(REFERERS)
        if referer:  # Only add if not empty
            headers['Referer'] = referer
    
    return headers


def get_chrome_headers() -> Dict[str, str]:
    """Get headers specifically mimicking Chrome browser."""
    chrome_ua = [ua for ua in USER_AGENTS if 'Chrome' in ua and 'Edg' not in ua]
    
    return {
        'User-Agent': random.choice(chrome_ua),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': random.choice(ACCEPT_LANGUAGES),
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }


def get_firefox_headers() -> Dict[str, str]:
    """Get headers specifically mimicking Firefox browser."""
    firefox_ua = [ua for ua in USER_AGENTS if 'Firefox' in ua]
    
    return {
        'User-Agent': random.choice(firefox_ua),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': random.choice(ACCEPT_LANGUAGES),
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'TE': 'trailers',
    }


if __name__ == "__main__":
    # Test headers
    print("Random Headers Example:\n")
    headers = get_random_headers()
    for key, value in headers.items():
        print(f"{key}: {value}")
    
    print("\n" + "="*60 + "\n")
    print("Chrome Headers Example:\n")
    chrome_headers = get_chrome_headers()
    for key, value in chrome_headers.items():
        print(f"{key}: {value}")
