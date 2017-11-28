import oss2
import yaml
import os
import pdb

def loadConfig():
    snap_conf_path = os.path.expanduser("~/.snap/")
    if not os.path.exists(snap_conf_path):
        os.mkdir(snap_conf_path)
    snap_conf_file =  os.path.expanduser("~/.snap/auth.conf")
    with open(snap_conf_file, 'r') as yaml_file:
        return yaml.load(yaml_file)

SNAP_CONF = loadConfig()
AUTH = oss2.Auth(SNAP_CONF['accesskeyid'], SNAP_CONF['accesskeysecret'])
BUCKET = oss2.Bucket(AUTH, SNAP_CONF['endpoint'], SNAP_CONF['bucket'])
