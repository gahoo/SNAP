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
    if not logger.handlers:
        logger.addHandler(handler)

    return logger

def new_log_file_handler(dbfile):
    (prefix, ext) =  os.path.splitext(dbfile)
    log_file = prefix + '.log'
    return logging.FileHandler(log_file)

def human_size(num):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f %sB" % (num, unit)
        num /= 1024.0
    return "%.1f %sB" % (num, 'Y')

def get_folder_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def question(msg):
    confirm = raw_input(msg)
    if confirm not in ['y', 'yes']:
        return False
    else:
        return True

concat = lambda x, y: x + y
