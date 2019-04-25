import yaml
import os
import pdb
from ..colorMessage import dyeFAIL

def loadAliConfig():
    ali_conf_path = os.path.expanduser("~/.snap/")
    if not os.path.exists(ali_conf_path):
        os.mkdir(ali_conf_path)
    ali_conf_file =  os.path.expanduser("~/.snap/ali.conf")
    with open(ali_conf_file, 'r') as yaml_file:
        return yaml.load(yaml_file)

try:
    ALI_CONF = loadAliConfig()
except IOError, e:
    print dyeFAIL(str(e))
    print "Please config aliyun bcs first with this command:\nsnap bcs config -accesskey_id <ACCESSKEY_ID> -accesskey_secret <ACCESSKEY_SECRET> -bucket <BUCKET> -region <REGION>"
    ALI_CONF = None
