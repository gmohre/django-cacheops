from __future__ import absolute_import
import warnings
import six
from time import time
import logging

from funcy import decorator, identity, memoize
import redis
import random

from .conf import settings

logger = logging.getLogger(__name__)

# Global circuit-breaker flag. Values:
#   None = circuit breaker closed, Redis calls are attempted.
#   <time_seconds> = circuit breaker open, Redis calls are skipped.
circuit_breaker_opened = None

if settings.CACHEOPS_DEGRADE_ON_FAILURE:
    @decorator
    def handle_connection_failure(call):
        """Skip Redis calls for a configurable period after a timeout."""
        global circuit_breaker_opened
        if circuit_breaker_opened:
            # Circuit breaker is open! Should we close it yet?
            if time() - circuit_breaker_opened > settings.REDIS_CIRCUIT_BREAKER_RESET_SECONDS:
                # Yes, let's close the circuit breaker
                logger.info("Closing Redis circuit breaker")
                circuit_breaker_opened = None
            else:
                # No, just skip this Redis call
                logger.debug("Redis circuit breaker is open! Skipping Redis call")
                return
        try:
            return call()
        except redis.RedisError as e:
            # Redis timed out! Let's open the circuit breaker
            logger.warn("The cacheops cache is unreachable! Error: %s" % e)
            logger.info("Opening Redis circuit breaker")
            circuit_breaker_opened = time()
        except Exception as e:
            logger.warn(e)
else:
    handle_connection_failure = identity


class SafeRedis(redis.StrictRedis):
    get = handle_connection_failure(redis.StrictRedis.get)

    """ Handles failover of AWS elasticache
    """
    def execute_command(self, *args, **options):
        try:
            return super(SafeRedis, self).execute_command(*args, **options)
        except redis.ResponseError as e:
            if "READONLY" not in e.message:
                raise
            connection = self.connection_pool.get_connection(args[0], **options)
            connection.disconnect()
            warnings.warn("Primary probably failed over, reconnecting")
            return super(SafeRedis, self).execute_command(*args, **options)

class LazyRedis(object):
    def _setup(self):
        Redis = SafeRedis if settings.CACHEOPS_DEGRADE_ON_FAILURE else redis.StrictRedis

        # Allow client connection settings to be specified by a URL.
        if isinstance(settings.CACHEOPS_REDIS, six.string_types):
            client = Redis.from_url(settings.CACHEOPS_REDIS)
        else:
            client = Redis(**settings.CACHEOPS_REDIS)

        object.__setattr__(self, '__class__', client.__class__)
        object.__setattr__(self, '__dict__', client.__dict__)

    def __getattr__(self, name):
        self._setup()
        return getattr(self, name)

    def __setattr__(self, name, value):
        self._setup()
        return setattr(self, name, value)


CacheopsRedis = SafeRedis if settings.CACHEOPS_DEGRADE_ON_FAILURE else redis.StrictRedis
try:
    # the conf could be a list of string
    # list would look like: ["redis://cache-001:6379/1", "redis://cache-002:6379/2"]
    # string would be: "redis://cache-001:6379/1,redis://cache-002:6379/2"
    redis_replica_conf = settings.CACHEOPS_REDIS_REPLICA
    if isinstance(redis_replica_conf, six.string_types):
        redis_replicas = map(redis.StrictRedis.from_url, redis_replica_conf.split(','))
    else:
        redis_replicas = map(redis.StrictRedis.from_url, redis_replica_conf)
except AttributeError as err:
    redis_client = LazyRedis()
else:
    class ReplicaProxyRedis(CacheopsRedis):
        """ Proxy `get` calls to redis replica.
        """
        def get(self, *args, **kwargs):
            try:
                redis_replica = random.choice(redis_replicas)
                return redis_replica.get(*args, **kwargs)
            except redis.ConnectionError:
                return super(ReplicaProxyRedis, self).get(*args, **kwargs)

    if isinstance(settings.CACHEOPS_REDIS, six.string_types):
        redis_client = ReplicaProxyRedis.from_url(settings.CACHEOPS_REDIS)
    else:
        redis_client = ReplicaProxyRedis(**settings.CACHEOPS_REDIS)

### Lua script loader

import re
import os.path

STRIP_RE = re.compile(r'TOSTRIP.*/TOSTRIP', re.S)

@memoize
def load_script(name, strip=False):
    filename = os.path.join(os.path.dirname(__file__), 'lua/%s.lua' % name)
    with open(filename) as f:
        code = f.read()
    if strip:
        code = STRIP_RE.sub('', code)
    return redis_client.register_script(code)
