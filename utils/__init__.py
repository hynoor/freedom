import json
import redis
from functools import wraps


cache = redis.StrictRedis('localhost')

def redis_cache(prefix='', ignore_first_arg=False, extra_cond=lambda: False, expire=1*3600):
    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            no_cache = kwargs.get('no_cache', False)
            params = args[1:] if ignore_first_arg else args
            params = '_'.join([str(param).lower() for param in params])
            key = kwargs.get('key', f'{prefix}_{params}')
            data = None if no_cache or extra_cond() else cache.get(key)
            if data is None:
                data = func(*args, **kwargs)
                if data:
                    print(f'update redis key: {key}')
                    cache.set(key, json.dumps(data), ex=expire)
            else:
                print(f'hit cache for key: {key}')
                data = json.loads(data)
            return data
        return wrapped
    return wrapper

