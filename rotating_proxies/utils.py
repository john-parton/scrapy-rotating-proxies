import re
from urllib.parse import urlparse


HAS_PROTOCOL_PATTERN = re.compile('^.+://')


def extract_proxy_hostport(proxy) -> str:
    """
    Return the hostport component from a given proxy:

    >>> extract_proxy_hostport('example.com')
    'example.com'
    >>> extract_proxy_hostport('http://www.example.com')
    'www.example.com'
    >>> extract_proxy_hostport('127.0.0.1:8000')
    '127.0.0.1:8000'
    >>> extract_proxy_hostport('127.0.0.1')
    '127.0.0.1'
    >>> extract_proxy_hostport('localhost')
    'localhost'
    >>> extract_proxy_hostport('zot:4321')
    'zot:4321'
    >>> extract_proxy_hostport('http://foo:bar@baz:1234')
    'baz:1234'
    """
    if not HAS_PROTOCOL_PATTERN.search(proxy):
        proxy = f'//{proxy}'

    parsed = urlparse(proxy)

    try:
        port = parsed.port
    except ValueError:
        return parsed.hostname
    else:
        return f'{parsed.hostname}:{parsed.port}'
