import oss2
import os
from . import ALI_CONF

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

endpoint = "http://oss-%s.aliyuncs.com" % ALI_CONF['region']
AUTH = oss2.Auth(ALI_CONF['accesskey_id'], ALI_CONF['accesskey_secret'])
BUCKET = oss2.Bucket(AUTH, endpoint, ALI_CONF['bucket'])
