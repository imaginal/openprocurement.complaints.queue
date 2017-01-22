# -*- coding: utf-8 -*-
import time
from functools import wraps

bool_dict = {'y': 1, 'n': 0, 'yes': 1, 'no': 0,
    'on': 1, 'off': 0, 'true': 1, 'false': 0}


def getboolean(value):
    try:
        return bool_dict[value.strip().lower()]
    except:
        return int(value or 0)


def retry(ExceptionToCheck=Exception, tries=5, delay=5, backoff=2, logger=None):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except ExceptionToCheck, e:
                    if logger:
                        logger.error("%s: %s, Retrying in %d seconds...",
                            type(e).__name__, str(e), mdelay)
                    for i in range(int(10 * mdelay)):
                        time.sleep(0.1)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry  # true decorator
    return deco_retry


def dump_error(e, client=None):
    out = "{}: {}".format(type(e).__name__, e)
    try:
        if hasattr(e, 'response'):
            response = getattr(e, 'response', None)
            status = getattr(response, 'status_int', None)
            headers = getattr(response, 'headers', None)
            out += " Status:" + str(status)
            out += " Headers:" + str(headers)
        if client:
            headers = getattr(client, 'headers', None)
            params = getattr(client, 'params', None)
            prefix = getattr(client, 'prefix_path')
            uri = getattr(client, 'uri')
            out += " RequestHeaders:" + str(headers)
            out += " RequestParams:" + str(params)
            out += " RequestURI:%s%s" % (uri, prefix)
    except:
        out += " (dump error)"
    return out
