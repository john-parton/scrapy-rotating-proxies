# -*- coding: utf-8 -*-
import logging
from functools import partial


from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.downloadmiddlewares.retry import get_retry_request
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
                # the get_retry_request helper method
                # should turn the exception into a "pretty" string
                return self._retry_ban(
                    request,
                    reason=exception,
                    spider=spider,
                    proxy=proxy,
                )

            # The exception here is usually "ignored request" ... I don't think
            # that means our proxy is actually good
            # So we shouldn't mark it as such here

    def _retry_ban(self, request, *, proxy, reason, spider):
        self.proxies.mark_dead(proxy)

        # ??? Doesn't look at meta ???
        # Not sure the best way to configure this
        max_retry_times = self.max_retry_times
        priority_adjust = self.priority_adjust

        return get_retry_request(
            request,
            reason=reason,
            spider=spider,
            max_retry_times=max_retry_times,
            priority_adjust=priority_adjust
        )


    def process_response(self, request, response, spider):

        proxy = request.meta.get("_rotating_proxy", None)

        # Only handle requests that were made with rotating proxy middleware
        # Almost completely copy/pasted from above
        if proxy is not None:

            ban = spider.response_is_ban(request, response)

            if ban:

                if response.body:
                    reason = f"ban/status/{response.status}"
                else:
                    reason = "ban/empty"

                return self._retry_ban(
                    request,
                    reason=reason,
                    spider=spider,
                    proxy=proxy
                )

            # Ban is already checked above and guarded here?
            if not ban and "cached" not in response.flags:
                self.proxies.mark_good(proxy)

        return response

    def log_stats(self):
        # TODO Make this better
        logger.info(str(self.proxies))

        # Should this be in its own looping method?
        self.crawler.stats.set_value(
            "proxies/mean_backoff", self.proxies.mean_backoff_amount
        )

        # Should this be in its own looping method?
        for status, count in self.proxies.get_status_counts().items():
            self.crawler.stats.set_value(
                f"proxies/{status}", count
            )
