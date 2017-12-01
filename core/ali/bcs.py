from batchcompute import Client
from . import ALI_CONF

endpoint = "batchcompute.%s.aliyuncs.com" % ALI_CONF['region']
CLIENT = Client(endpoint, ALI_CONF['accesskey_id'], ALI_CONF['accesskey_secret'], human_readable=True)
