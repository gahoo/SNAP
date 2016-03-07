import yaml
import os

class App(object):
    """Everything about App"""
    def __init__(self, app_path):
        super(App, self).__init__()
        self.app_path = app_path
        self.config_file = app_path + '/config.yaml'
        self.parameters = {}
        self.config = {
            'app':{
                'package': "package_name",
                'category': "category of tools",
                'homepage': None,
                'version': 1,
                'name': "app name",
                'alias': "alias name",
                'description': "",
                'tutorial': "",
                'document_author': "",
                'requirements': {
                    'container':{
                        'type': "docker",
                        'image': 'user/image_name'
                    },
                    'resources': {
                        'cpu': 4,
                        'mem': '4096m',
                        'network': False,
                        'port': [],
                        'disk': '10000m'
                    }
                },
                'inputs':{
                    'bam':{
                        'hint': "bwa mem bam",
                        'type': 'file',
                        'required': True,
                        'minitems': 1,
                        'maxitems': 1,
                        'item':{
                            'separator': " "
                        },
                        'formats': ['bam']
                    }
                },
                'outputs':{
                    'results': {
                        'type': 'file',
                        'required': True,
                        'minitems': 1,
                        'maxitems': 1,
                        'item':{
                            'separator': " "
                        },
                        'formats': ['tgz']
                    }
                },
                'parameters':{
                    'workspace': {
                        'separator': "",
                        'prefix': "",
                        'type': 'string',
                        'required': True,
                        'default': '/data/project/id',
                        'quotes': False,
                        'hint': 'working space'
                    }
                },
                'cmd_template': "mkdir -p {{parameters.workspace}};{{'\\n'}}"
            }}

    def load(self):
        with open(self.config_file, 'r') as config_file:
            self.config = yaml.load(config_file)

    def new(self):
        createDir = lambda folder : os.makedirs("%s/%s" % (self.app_path, folder))
        touchFile = lambda filename: open("%s/%s" % (self.app_path, filename), 'a').close()

        map(createDir, ['bin', 'lib', 'test'])
        map(touchFile, ['Dockerfile', 'README.md'])
        with open(self.config_file, 'w') as config_file:
            # config_file.write(self.sample_config)
            yaml.dump(self.config, config_file, default_flow_style=False)

    def check(self):
        """
        check if the config file is good to go.
        """
        pass

    def build(self):
        pass

    def test(self):
        pass

    def run(self):
        pass

    def dump_parameter(self):
        pass
