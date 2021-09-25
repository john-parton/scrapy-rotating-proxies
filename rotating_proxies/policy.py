# -*- coding: utf-8 -*-
import warnings

from scrapy.exceptions import IgnoreRequest


class BanDetectionPolicy:
    """
    Default ban detection rules.
    Can be used as a mixin on a crawler
    """

    NOT_BAN_STATUSES = {200, 301, 302}
    NOT_BAN_EXCEPTIONS = (IgnoreRequest, )

    def response_is_ban(self, request, response):
        return resonse.status not in self.NOT_BAN_STATUSES or (
            response.status == 200 and not response.body
        )

    def exception_is_ban(self, request, exception):
        if isinstance(exception, self.NOT_BAN_EXCEPTIONS):
            return False

        # warnings.warn(f"In exception_is_ban, ignoring exception: {exception}")
        return True
