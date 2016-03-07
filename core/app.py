import yaml

class App(object):
    """Everything about App"""
    def __init__(self, app_path):
        super(App, self).__init__()
        self.app_path = app_path
        self.parameters = {}
        self.config = {}

    def load(self, app_path):
        config_file = file(app_path, 'r')
        self.config = yaml.load(config_file)

    def new(self):
        pass

    def build(self):
        pass

    def test(self):
        pass

    def run(self):
        pass

    def dump_parameter(self):
        pass
