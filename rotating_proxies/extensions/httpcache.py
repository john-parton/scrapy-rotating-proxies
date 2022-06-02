from scrapy.extensions.httpcache import DummyPolicy, RFC2616Policy

# This mixin can be used to create a new httpcache policy
# which invalidates the cache if any retries are used
# It is almost always necessary to use this if using
# the rotating proxy middleware in conjuction with the cache
# middleware
class RetryPolicyMixin:
    def is_cached_response_fresh(self, cachedresponse, request):
        # If the incoming request has any retries attached to it
        # assume that the cache is not fresh
        if request.meta.get("retry_times", 0) > 0:
            return False
        return super().is_cached_response_fresh(cachedresponse, request)

    def is_cached_response_valid(self, cachedresponse, response, request):
        # If the incoming request has any retries attached to it
        # we need to overwrite the cache no matter what
        if request.meta.get("retry_times", 0) > 0:
            return False
        return super().is_cached_response_valid(cachedresponse, response, request)


# For convenience
class RetryPolicy(RetryPolicyMixin, DummyPolicy):
    pass


class RetryRFC2616Policy(RetryPolicyMixin, RFC2616Policy):
    pass
