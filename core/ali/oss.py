import oss2
import os
from . import ALI_CONF

def oss2key(destination):
    prefix = os.path.join('oss://', BUCKET.bucket_name)
    key = destination.replace(prefix, '').strip('/')
    return key

endpoint = "http://oss-%s.aliyuncs.com" % ALI_CONF['region']
AUTH = oss2.Auth(ALI_CONF['accesskey_id'], ALI_CONF['accesskey_secret'])
BUCKET = oss2.Bucket(AUTH, endpoint, ALI_CONF['bucket'])
