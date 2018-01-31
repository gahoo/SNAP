import yaml
import logging
import os

def loadYaml(filename):
    with open(filename, 'r') as yaml_file:
        return yaml.load(yaml_file)

def dumpYaml(filename, obj):
    with open(filename, 'w') as yaml_file:
        yaml.dump(obj, yaml_file, default_flow_style=False)

def write(filename, content):
    with open(filename, 'w') as output_file:
        output_file.write(content)

def new_logger(name, handler = logging.StreamHandler()):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = "%(asctime)-15s\t%(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

def new_log_file_handler(dbfile):
    (prefix, ext) =  os.path.splitext(dbfile)
    log_file = prefix + '.log'
    return logging.FileHandler(log_file)
