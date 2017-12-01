import yaml
import os
import pdb

def loadAliConfig():
    ali_conf_path = os.path.expanduser("~/.snap/")
    if not os.path.exists(ali_conf_path):
        os.mkdir(ali_conf_path)
    ali_conf_file =  os.path.expanduser("~/.snap/ali.conf")
    with open(ali_conf_file, 'r') as yaml_file:
        return yaml.load(yaml_file)

ALI_CONF = loadAliConfig()
