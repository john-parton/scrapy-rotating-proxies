# -*- coding: utf-8 -*-
import typing
import warnings

from scrapy.exceptions import IgnoreRequest



class BanDetectionMixin:
    """
    Default ban detection rules.
    Needs to be mixed into a crawler or an alternate
    implemntation of response_is_ban and exception_is_ban
    must be provided
    """

    BAN_DETECTION_EMPTY_200_OK: typing.ClassVar[bool] = False
    BAN_DETECTION_OK_STATUSES = {200, 301, 302}
    BAN_DETECTION_OK_EXCEPTIONS = (IgnoreRequest, )

    def response_is_ban(self, request, response):
        if not self.BAN_DETECTION_EMPTY_200_OK and response.status == 200 and not response.body:
            return True

        return response.status not in self.BAN_DETECTION_OK_STATUSES

    def exception_is_ban(self, request, exception):
        if isinstance(exception, self.BAN_DETECTION_OK_EXCEPTIONS):
            return False

        # warnings.warn(f"In exception_is_ban, ignoring exception: {exception}")
        return True
