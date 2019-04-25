import yaml
import os
import pdb
from ..colorMessage import dyeFAIL

def loadDingTalkConfig():
    ding_conf_path = os.path.expanduser("~/.snap/")
    if not os.path.exists(ding_conf_path):
        os.mkdir(ding_conf_path)
    ding_conf_file =  os.path.expanduser("~/.snap/dingtalk.conf")
    with open(ding_conf_file, 'r') as yaml_file:
        return yaml.load(yaml_file)

try:
    DINGTALK_CONF = loadDingTalkConfig()
except IOError, e:
    print dyeFAIL(str(e))
    print "dingtalk robot access_token is missing."
    DINGTALK_CONF = None
