import oss2
import os
from . import ALI_CONF
from oss2.exceptions import NoSuchKey
from ..colorMessage import dyeWARNING

def oss2key(destination):
    prefix = os.path.join('oss://', BUCKET.bucket_name)
    key = destination.replace(prefix, '').strip('/')
    return key

class OSSkeys(object):
    def __init__(self, array, step = 1000):
        self.idx = 0
        self.array = array
        self.step = step
        self.length = len(array)
        self.n = self.length / self.step

    def __iter__(self):
        return self

    def next(self):
        if self.idx < self.n:
            val = self.array[self.idx * self.step:self.step * (self.idx + 1)]
            self.idx = self.idx + 1
            return val
        elif self.idx == self.n:
            val = self.array[self.idx * self.step:]
            self.idx = self.idx + 1
            return val
        else:
            raise StopIteration()


def read_object(key):
    try:
        meta = BUCKET.get_object_meta(key)
    except Exception, e:
        if not isinstance(e, NoSuchKey):
            raise e
        print dyeWARNING("{key} not found.".format(key=key))
        return ''

    if meta.content_length > 100 * 1024:
        byte_range = (None, 10 * 1024)
    else:
        byte_range = None

    return BUCKET.get_object(key, byte_range).read()

if ALI_CONF:
    endpoint = "http://oss-%s.aliyuncs.com" % ALI_CONF['region']
    AUTH = oss2.Auth(ALI_CONF['accesskey_id'], ALI_CONF['accesskey_secret'])
    BUCKET = oss2.Bucket(AUTH, endpoint, ALI_CONF['bucket'])
else:
    AUTH = None
    BUCKET = None
