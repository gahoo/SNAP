import yaml

def loadYaml(filename):
    with open(filename, 'r') as yaml_file:
        return yaml.load(yaml_file)

def dumpYaml(filename, obj):
    with open(filename, 'w') as yaml_file:
        yaml.dump(obj, yaml_file, default_flow_style=False)

def write(filename, content):
    with open(filename, 'w') as output_file:
        output_file.write(content)
