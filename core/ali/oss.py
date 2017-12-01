import oss2
from . import ALI_CONF

endpoint = "http://oss-%s.aliyuncs.com" % ALI_CONF['region']
AUTH = oss2.Auth(ALI_CONF['accesskey_id'], ALI_CONF['accesskey_secret'])
BUCKET = oss2.Bucket(AUTH, endpoint, ALI_CONF['bucket'])
