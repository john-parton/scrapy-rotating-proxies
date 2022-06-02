import logging
import math
import random
import typing
import time

import attr


logger = logging.getLogger(__name__)


@attr.s
class ExponentialBackoff:
    attempts: int = attr.ib(default=0)
    # The "base" here should probably have some relationship to the number
    # of proxies we are trying to test
    # Also probably ratio of "actually good" versus "actually bad" proxies provided
    # as well as the expected response time for a given proxy
    # If we're reanimating proxies faster than we can check them
    # then the backing off doesn't really do anything
    # Jitter is also a factor obviously
    # 5 minutes
    # 10 minutes
    # 20 minutes
    # 40 minutes
    # ...
    base: int = attr.ib(default=600, on_setattr=attr.setters.frozen)
    max_amount: int = attr.ib(default=24 * 60 * 60, on_setattr=attr.setters.frozen)
    amount: float = attr.ib(default=0.0)
    time: typing.Optional[float] = attr.ib(default=None)

    def __attrs_post_init__(self):
        # this is a numerically stable version of
        # min(cap, base * 2 ** attempt)
        # Should be ceil? floor?
        self._max_attempts = math.log(self.max_amount / self.base, 2)

    def _get_amount(self):
        # Should be <= or < ?
        return (
            self.base * 2**self.attempts
            if self.attempts < self._max_attempts
            else self.max_amount
        )

    def __call__(self):
        """Exponential backoff time"""

        self.amount = self._get_amount()
        self.time = time.monotonic() + self.amount
        self.attempts += 1

    def reset(self):
        self.attempts = 0


class ExponentialBackoffWithJitter(ExponentialBackoff):
    def _get_amount(self):
        return random.uniform(0, super()._get_amount())
