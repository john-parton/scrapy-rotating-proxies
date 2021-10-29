# -*- coding: utf-8 -*-
import collections
import logging
import functools
import itertools as it
import math
import random
import time
import typing
from statistics import fmean
from urllib.parse import urlparse
import enum

import attr
from scrapy.utils.url import add_http_if_no_scheme

from .backoff import ExponentialBackoff, ExponentialBackoffWithJitter


logger = logging.getLogger(__name__)


class ProxyManager:
    """
    Expiring proxies container.

    A proxy can be in 3 states:

    * good;
    * dead;
    * unchecked.

    Initially, all proxies are in 'unchecked' state.
    When a request using a proxy is successful, this proxy moves to 'good'
    state. When a request using a proxy fails, proxy moves to 'dead' state.

    For crawling only 'good' and 'unchecked' proxies are used.

    'Dead' proxies move to 'unchecked' after a timeout (they are called
    'reanimated'). This timeout increases exponentially after each
    unsuccessful attempt to use a proxy.
    """

    def __init__(self, proxy_list):

        proxy_list = map(str.rstrip, proxy_list)
        proxy_list = map(add_http_if_no_scheme, proxy_list)

        # Remove dupes
        proxy_list = set(proxy_list)

        # TODO Restore actual custom backoff function# TODO Re-enable passing backoff
        self.proxies = list(map(Proxy.from_string, proxy_list))

    def _clear_cache(self):
        try:
            del self._cached
        except AttributeError:
            pass

    @functools.cached_property
    def _cached(self):
        proxies = [
            proxy for proxy in self.proxies if proxy.status != ProxyStatus.DEAD
        ]

        weights = list(
            it.accumulate(
                proxy.weight for proxy in proxies
            )
        )

        return proxies, weights

    def get_random(self) -> typing.Optional['Proxy']:
        """Return a random available proxy (either good or unchecked)"""
        # This list comprehension and random choices
        # call is surprisingly expensive
        # It actually causes us to be CPU-bound in the case where
        # we're pulling everything from cache
        available, weights = self._cached

        if not available:
            return None

        return random.choices(
            available,
            cum_weights=weights
        )[0]

    def reanimate(self, slots):
        """Move dead proxies to unchecked if a backoff timeout passes"""
        number_reanimated = 0


        # Why
        delays = [
            (slot.delay if slot else None) for slot in (
                slots.get(proxy.slot_key, None) for proxy in self.proxies
            )
        ]

        try:
            average_delay = fmean(
                delay for delay in delays if delay is not None
            )
        except ValueError:
            average_delay = 0.100

        min_delay = min(
            (delay for delay in delays if delay is not None), default=0.100
        )

        # TODO Log min dealay / average delay

        # For logging/comparing against non-weighted random choice
        # try:
        #     average_good_delay = fmean(
        #         (average_delay if delay is None else delay)
        #         for proxy, delay in zip(self.proxies, delays) if proxy.status == ProxyStatus.GOOD
        #     )
        #
        #     print(f"average_good_delay: {average_good_delay}")
        # except ValueError:
        #     pass


        print(f"average_delay: {average_delay}")
        print(f"min_delay: {min_delay}")


        for proxy, delay in zip(self.proxies, delays):

            # Inverse delay is approximately "requests per second"
            if delay is None:
                if proxy.status == ProxyStatus.UNCHECKED:
                    # Unchecked proxies are ten times as likely to be queued up as the fastest
                    # checked proxy
                    # could do 1 / min_delay if we wanted to balance checking proxies with fast proxies
                    proxy.weight = 10 / min_delay
                    print(f"{proxy.url!r} is unchecked, setting weight to {proxy.weight}")
                # Re-animated or good missing weight somehow?
                else:
                    proxy.weight = 1 / average_delay
            else:
                proxy.weight = 1 / delay

            if proxy.reanimate():
                number_reanimated += 1

        self._clear_cache()

        return number_reanimated

    def reset(self):
        """Mark all dead proxies as unchecked"""
        for proxy in self.proxies:
            proxy.reset()

        self._clear_cache()

    def mark_good(self, proxy):
        invalidate = proxy.mark_good()

        if invalidate:
            self._clear_cache()

    def mark_dead(self, proxy):
        invalidate = proxy.mark_dead()

        if invalidate:
            self._clear_cache()

    @property
    def mean_backoff_amount(self) -> float:
        try:
            return fmean(
                proxy.backoff.amount for proxy in self.proxies if proxy.status == ProxyStatus.DEAD
            )
        except ValueError:
            return 0.0

    def get_status_counts(self):
        return collections.Counter(proxy.status for proxy in self.proxies)

    def __str__(self):
        status_counter = self.get_status_counts()

        return (
            "Proxies("
            f"good: {status_counter[ProxyStatus.GOOD]}, "
            f"dead: {status_counter[ProxyStatus.DEAD]}, "
            f"unchecked: {status_counter[ProxyStatus.UNCHECKED]}, "
            f"reanimated: {status_counter[ProxyStatus.REANIMATED]}, "
            f"mean backoff amount: {int(self.mean_backoff_amount)}s"
            ")"
        )


class ProxyStatus(enum.Enum):
    UNCHECKED = 'unchecked'
    GOOD = 'good'
    DEAD = 'dead'
    REANIMATED = 'reanimated'


# TODO Maybe try to keep track of how often it's banned as a percentage of request
# Or also "uptime"
@attr.s(hash=False, eq=False)
class Proxy:
    url: str = attr.ib(on_setattr=attr.setters.frozen)
    slot_key: str = attr.ib(on_setattr=attr.setters.frozen)
    status: ProxyStatus = attr.ib(default=ProxyStatus.UNCHECKED)

    # For retrying
    backoff: ExponentialBackoff = attr.ib(factory=ExponentialBackoffWithJitter)

    # For random weighted selection
    weight: float = attr.ib(default=1.0)

    @classmethod
    def from_string(cls, proxy: str) -> 'ProxyState':
        """
        Return downloader slot for a proxy.

        By default it doesn't take port in account, i.e. all proxies with
        the same hostname / ip address share the same slot, unless
        the hostname is "localhost" in which case it does
        """
        # FIXME: an option to use website address as a part of slot as well?
        parsed = urlparse(proxy)

        if parsed.hostname == "localhost":
            slot_key = f"{parsed.hostname}:{parsed.port}"
        else:
            slot_key = parsed.hostname

        # TODO Consider making proxy canonical
        # Case-sensitivity and other quirks might cause us pain
        return cls(
            url=proxy,
            slot_key=slot_key
        )

    def __eq__(self, other):
        return hasattr(other, 'url') and self.url == other.url

    def __hash__(self):
        return hash(self.url)

    def reset(self):
        """Mark self as unchecked"""
        self.status = ProxyStatus.UNCHECKED
        self.backoff.reset()

    # Return True if cache needs to be invalidated
    def mark_dead(self) -> bool:
        """Mark self as dead"""
        # if proxy not in self.proxies:
        #     logger.warn(f"Proxy <{proxy}> was not found in proxies list")
        #     return

        if self.status != ProxyStatus.DEAD:
            logger.debug(f"{self.status} proxy became DEAD: <{self}>")
            self.status = ProxyStatus.DEAD
            self.backoff()
            return True
        else:
            logger.debug(f"Proxy <{self}> is DEAD")
            return False

    # Return True if cache needs to be invalidated
    def mark_good(self) -> bool:
        """Mark a self as good"""

        if self.status != ProxyStatus.GOOD:
            logger.debug(f"Proxy <{self}> is GOOD")

            self.status = ProxyStatus.GOOD
            self.backoff.reset()
            return True
        else:
            return False

    def reanimate(self) -> bool:
        """Reanimate self as reanimated

        Return True if reanimated"""

        if self.status == ProxyStatus.DEAD and self.backoff.time <= time.monotonic():
            self.status = ProxyStatus.REANIMATED
            return True
        else:
            return False
