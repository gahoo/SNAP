import yaml
import os

class App(object):
    """Everything about App"""
    def __init__(self, app_path):
        super(App, self).__init__()
        self.app_path = app_path
        self.config_file = app_path + '/config.yaml'
        self.parameters = {}
        self.config = {}

    def load(self):
        with open(self.config_file, 'r') as config_file:
            self.config = yaml.load(config_file)

    def new(self):
        for folder in ['bin', 'lib', 'test']:
            os.makedirs("%s/%s" % (self.app_path, folder))
        open(self.app_path + '/Dockerfile', 'a').close()
        with open(self.config_file, 'w') as config_file:
            yaml.dump(self.config, config_file)

    def build(self):
        pass

    def test(self):
        pass

    def run(self):
        pass

    def dump_parameter(self):
        pass
