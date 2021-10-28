# -*- coding: utf-8 -*-
import logging
from functools import partial


from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.downloadermiddlewares.httpcache import HttpCacheMiddleware
from scrapy.exceptions import CloseSpider, NotConfigured
from scrapy import signals
from scrapy.utils.misc import load_object
from twisted.internet import task

from .expire import ProxyManager, ProxyStatus


logger = logging.getLogger(__name__)


class RotatingProxyMiddleware:
    """
    Scrapy downloader middleware which choses a random proxy for each request.

    To enable it, add it and BanDetectionMiddleware
    to DOWNLOADER_MIDDLEWARES option::

        DOWNLOADER_MIDDLEWARES = {
            # ...
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
            # ...
        }

    It keeps track of dead and alive proxies and avoids using dead proxies.
    Proxy is considered dead if request.meta['_ban'] is True, and alive
    if request.meta['_ban'] is False; to set this meta key use
    BanDetectionMiddleware.

    Dead proxies are re-checked with a randomized exponential backoff.

    By default, all default Scrapy concurrency options (DOWNLOAD_DELAY,
    AUTOTHROTTLE_..., CONCURRENT_REQUESTS_PER_DOMAIN, etc) become per-proxy
    for proxied requests when RotatingProxyMiddleware is enabled.
    For example, if you set CONCURRENT_REQUESTS_PER_DOMAIN=2 then
    spider will be making at most 2 concurrent connections to each proxy.

    Settings:

    * ``ROTATING_PROXY_LIST``  - a list of proxies to choose from;
    * ``ROTATING_PROXY_LIST_PATH``  - path to a file with a list of proxies;
    * ``ROTATING_PROXY_LOGSTATS_INTERVAL`` - stats logging interval in seconds,
      30 by default;
    * ``ROTATING_PROXY_CLOSE_SPIDER`` - When True, spider is stopped if
      there are no alive proxies. If False (default), then when there is no
      alive proxies all dead proxies are re-checked.
    * ``ROTATING_PROXY_PAGE_RETRY_TIMES`` - a number of times to retry
      downloading a page using a different proxy. After this amount of retries
      failure is considered a page failure, not a proxy failure.
      Think of it this way: every improperly detected ban cost you
      ``ROTATING_PROXY_PAGE_RETRY_TIMES`` alive proxies. Default: 5.
    * ``ROTATING_PROXY_BACKOFF_BASE`` - base backoff time, in seconds.
      Default is 300 (i.e. 5 min).
    * ``ROTATING_PROXY_BACKOFF_CAP`` - backoff time cap, in seconds.
      Default is 3600 (i.e. 60 min).
    """

    # Because the retry is processed by this middleware and not the
    # retry middleware, we need to manually adjust the priority
    priority_adjust = -1

    def __init__(self, crawler):
        s = crawler.settings
        proxy_path = s.get("ROTATING_PROXY_LIST_PATH", None)

        if proxy_path is not None:
            with open(proxy_path, "rt", encoding="utf8") as f:
                proxy_list = map(str.strip, f)
                proxy_list = filter(None, proxy_list)
                proxy_list = list(proxy_list)
        else:
            proxy_list = s.getlist("ROTATING_PROXY_LIST")

        if not proxy_list:
            raise NotConfigured()

        self.proxies = ProxyManager(proxy_list)
        self.logstats_interval = s.getfloat("ROTATING_PROXY_LOGSTATS_INTERVAL", 30)
        self.reanimate_interval = 5  # Hard-coded
        self.stop_if_no_proxies = s.getbool("ROTATING_PROXY_CLOSE_SPIDER", False)
        self.max_proxy_retries = s.getint("ROTATING_PROXY_PAGE_RETRY_TIMES", 5)
        self.crawler = crawler
        self.log_task = None
        self.reanimate_task = None

        crawler.signals.connect(self.engine_started, signal=signals.engine_started)
        crawler.signals.connect(self.engine_stopped, signal=signals.engine_stopped)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def engine_started(self):
        if self.logstats_interval:
            self.log_task = task.LoopingCall(self.log_stats)
            self.log_task.start(self.logstats_interval, now=True)

        if self.reanimate_interval:
            self.reanimate_task = task.LoopingCall(self.reanimate_proxies)
            # Also sets weights for randomc choice
            self.reanimate_task.start(self.reanimate_interval, now=False)

    def engine_stopped(self):
        if self.log_task and self.log_task.running:
            self.log_task.stop()

        if self.reanimate_task and self.reanimate_task.running:
            self.reanimate_task.stop()

    def reanimate_proxies(self):
        n_reanimated = self.proxies.reanimate(
            slots=self.crawler.engine.downloader.slots
        )

        if n_reanimated:
            logger.debug("%s proxies moved from 'dead' to 'reanimated'", n_reanimated)

    def process_request(self, request, spider):
        # Proxy was manually specified
        if "proxy" in request.meta and not request.meta.get("_rotating_proxy"):
            return

        proxy = self.proxies.get_random()

        if not proxy:
            if self.stop_if_no_proxies:
                raise CloseSpider("no_proxies")
            else:
                logger.warn("No proxies available; marking all proxies " "as unchecked")
                self.proxies.reset()
                proxy = self.proxies.get_random()
                if proxy is None:
                    logger.error("No proxies available even after a reset.")
                    raise CloseSpider("no_proxies_after_reset")

        request.meta["proxy"] = proxy.url
        request.meta["download_slot"] = proxy.slot_key
        # Flag which indicates proxy was randomly assigned
        request.meta["_rotating_proxy"] = proxy

    def process_exception(self, request, exception, spider):

        proxy = request.meta.get("_rotating_proxy", None)

        if proxy is not None:

            ban = spider.exception_is_ban(request, exception)

            if ban:
                exception_class = exception.__class__
                self.crawler.stats.inc_value(
                    f"bans/error/{exception_class.__module__}.{exception_class.__name__}"
                )

                return self._handle_ban(request, proxy)


    def process_response(self, request, response, spider):

        proxy = request.meta.get("_rotating_proxy", None)

        # Only handle requests that were made with rotating proxy middleware
        if proxy is not None:

            ban = spider.response_is_ban(request, response)

            if ban:
                self.crawler.stats.inc_value(f"bans/status/{response.status}")
                if not response.body:
                    self.crawler.stats.inc_value("bans/empty")

                return self._handle_ban(request, proxy) or response

            else:
                if 'cached' not in response.flags:
                    self.proxies.mark_good(proxy)

        return response

    def _handle_ban(self, request, spider):

        proxy = request.meta["_rotating_proxy"]
        assert proxy

        self.proxies.mark_dead(proxy)

        # On _every_ result?
        # Maybe just call this every now and then?
        # Why isn't this in the log_stats looping function?
        for status, count in self.proxies.get_status_counts().items():
            self.crawler.stats.set_value(
                f"proxies/{status}", count
            )

        self.crawler.stats.set_value(
            "proxies/mean_backoff", self.proxies.mean_backoff_amount
        )

        return self._retry(request, spider)

    def _retry(self, request, spider):
        retries = request.meta.get("proxy_retry_times", 0) + 1
        max_proxy_retries = request.meta.get(
            "max_proxy_retries", self.max_proxy_retries
        )


        if retries <= max_proxy_retries:
            # TODO log "reason" like RetryMiddleware
            logger.debug(
                "Retrying %(request)s with another proxy "
                "(failed %(retries)d times, "
                "max retries: %(max_proxy_retries)d)",
                {
                    "request": request,
                    "retries": retries,
                    "max_proxy_retries": max_proxy_retries,
                },
                extra={"spider": spider},
            )

            retryreq = request.copy()
            retryreq.meta["proxy_retry_times"] = retries
            # It's not _ideal_ to pass 'dont_cache' in the meta here
            # That _will_ get the request to pass back through the HttpCache
            # layer, however, when the response comes back, it won't cache
            # that either
            # The solution is to use a caching policy. Refer to RotatingProxyHttpCacheMiddleware
            # below
            # retryreq.meta["dont_cache"] = True
            # Because the retry is processed by this middleware and not the
            # retry middleware, we need to manually adjust the priority
            retryreq.priority = request.priority + request.meta.get('priority_adjust', self.priority_adjust)
            retryreq.dont_filter = True
            return retryreq
        else:
            logger.debug(
                "Gave up retrying %(request)s (failed %(retries)d "
                "times with different proxies)",
                {"request": request, "retries": retries},
                extra={"spider": spider},
            )

    def log_stats(self):
        # Make this better
        logger.info(str(self.proxies))



class RotatingProxyHttpCacheMiddleware(HttpCacheMiddleware):
    # This is a replacement Middleware for the HttpCacheMiddleware which
    # lets our retry'd requests go through again
    # BUT the responses returned get to go into the cache
    # So if we crawl again, the first request will use the cache

    # No type annotations
    def process_request(self, request, spider):
        if request.meta.get('proxy_retry_times', 0):
            return

        return super().process_request(request, spider)
