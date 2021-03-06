import oss2
import os
import time
import hashlib
from . import ALI_CONF
from oss2.exceptions import NoSuchKey
from ..colorMessage import dyeWARNING, dyeFAIL

def oss2key(destination):
    prefix = os.path.join('oss://', BUCKET.bucket_name)
    key = destination.replace(prefix, '').lstrip('/')
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


def read_object(key, byte_range = None, warn=True, full=False):
    try:
        meta = BUCKET.get_object_meta(key)
    except Exception, e:
        if not isinstance(e, NoSuchKey):
            raise e
        if warn:
            print dyeWARNING("{key} not found.".format(key=key))
        return ''

    if byte_range is None and not full and meta.content_length > 100 * 1024:
        byte_range = (None, 10 * 1024)

    return BUCKET.get_object(key, byte_range).read()

def is_object_exists(destination):
    key = oss2key(destination)
    return BUCKET.object_exists(key)

def is_size_differ_and_newer(source, destination):
    key = oss2key(destination)
    meta = BUCKET.get_object_meta(key)
    source_size = os.path.getsize(source)
    if source_size != meta.content_length:
        msg = 'Warning: {source}({source_size}) size differ from {destination}({destination_size})'.format(
            source=source, source_size=source_size, destination=destination, destination_size=meta.content_length)
        if int(time.time()) > meta.last_modified:
            print dyeFAIL(msg)
            return True
        else:
            raise ValueError(msg + ' but not newer')
    else:
        return False

def is_source_newer(source, destination):
    key = oss2key(destination)
    meta = BUCKET.get_object_meta(key)
    return int(int(os.path.getmtime(source))) > meta.last_modified

def is_size_differ(source, destination):
    key = oss2key(destination)
    meta = BUCKET.get_object_meta(key)
    return os.path.getsize(source) != meta.content_length

def is_md5_differ(source, destination):
    key = oss2key(destination)
    meta = BUCKET.get_object_meta(key)
    if '-' in meta.etag:
        print dyeFAIL('Warnning: {destination} is a Multipart file, has no md5sum on oss.')
        return False
    return md5(source) != meta.etag.lower()

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

if ALI_CONF:
    endpoint = "http://oss-%s.aliyuncs.com" % ALI_CONF['region']
    AUTH = oss2.Auth(ALI_CONF['accesskey_id'], ALI_CONF['accesskey_secret'])
    BUCKET = oss2.Bucket(AUTH, endpoint, ALI_CONF['bucket'])
else:
    AUTH = None
    BUCKET = None
