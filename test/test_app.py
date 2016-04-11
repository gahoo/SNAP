import unittest
from core.app import App, AppParameter, AppFile
import shutil

class TestAppParameter(unittest.TestCase):
    """
    test for App Parameters
    """
    def test_check_error_parameter_type(self):
        param = {'type': 'error type'}
        self.assertRaises(TypeError, AppParameter, param)

    def test_number(self):
        num_param = {
            'separator': " ",
            'prefix': "-n",
            'type': 'number',
            'required': True,
            'minvalue': 0,
            'maxvalue': 100,
            'hint': 'number test',
            'default': 6
        }
        param = AppParameter(num_param)
        self.assertEqual(param.__str__(), '-n 6')

    def test_string(self):
        string_param = {
            'separator': " ",
            'prefix': "-db",
            'type': 'string',
            'required': True,
            'quotes': False,
            'hint': 'working space',
            'default': 'nt',
            'quotes': False,
            'hint': 'working space'
        }
        param = AppParameter(string_param)
        self.assertEqual(param.__str__(), '-db nt')
        string_param['quotes'] = True
        param = AppParameter(string_param)
        self.assertEqual(param.__str__(), "-db 'nt'")

    def test_array(self):
        array_param = {
            "separator": '=',
            "prefix": '-taxid',
            "type": 'array',
            "required": True,
            "minitems": 1,
            "maxitems": 100,
            "item":{
                "type": 'string',
                "item_quotes": True,
                "separator": ' ',
                "is_split": True,
                },
            "hint": 'array test',
            "default": ['4', '5', '6', '7']
        }
        param = AppParameter(array_param)
        self.assertEqual(param.__str__(), "-taxid='4' -taxid='5' -taxid='6' -taxid='7'")
        # print param
        (array_param['item']['is_split'], array_param['item']['item_quotes']) = (True, False)
        param = AppParameter(array_param)
        self.assertEqual(param.__str__(), "-taxid=4 -taxid=5 -taxid=6 -taxid=7")
        # print param
        (array_param['item']['is_split'], array_param['item']['item_quotes']) = (False, True)
        param = AppParameter(array_param)
        self.assertEqual(param.__str__(), "-taxid='4' '5' '6' '7'")
        # print param
        (array_param['item']['is_split'], array_param['item']['item_quotes']) = (False, False)
        param = AppParameter(array_param)
        self.assertEqual(param.__str__(), "-taxid=4 5 6 7")
        # print param

        array_param['item']['separator'] = ','
        param = AppParameter(array_param)
        self.assertEqual(param.__str__(), "-taxid=4,5,6,7")
        # print param
        array_param['separator'] = ' '
        param = AppParameter(array_param)
        self.assertEqual(param.__str__(), "-taxid 4,5,6,7")
        # print param

    def test_flag(self):
        flag_param = {
            'separator': ' ',
            'prefix': '-pe',
            'type': 'flag',
            'required': True,
            'hint': 'flag test',
            'default': True
        }
        param = AppParameter(flag_param)
        self.assertEqual(param.__str__(), "-pe")
        flag_param['value']=False
        param = AppParameter(flag_param)
        self.assertEqual(param.__str__(), "")

    def test_boolean(self):
        boolean_param = {
            'separator': ' ',
            'prefix': '-pe',
            'type': 'boolean',
            'required': True,
            'hint': 'boolean test',
            'default': True
        }
        param = AppParameter(boolean_param)
        self.assertEqual(param.__str__(), "-pe True")
        boolean_param['value']=False
        param = AppParameter(boolean_param)
        self.assertEqual(param.__str__(), "-pe False")

class TestAppFile(unittest.TestCase):
    """docstring for TestAppFile"""
    def setUp(self):
        self.file = {
            'type': 'file',
            'required': True,
            'minitems': 1,
            'maxitems': 1,
            'item':{
                'separator': " "
            },
            'formats': ['tgz']
        }

    def test_path(self):
        output = AppFile(self.file)
        self.assertEqual(output.path[:10], "/var/data/")
        self.assertEqual(output.path[-3:], "tgz")

    def test_enid(self):
        output = AppFile(self.file)
        self.assertEqual(len(output.enid), 32)

    def test_format(self):
        self.file['formats'] = 'bam'
        output = AppFile(self.file)
        self.assertEqual(output.path[-3:], "bam")

    def test_name(self):
        self.file['name'] = '/path/to/data'
        output = AppFile(self.file)
        self.assertEqual(output.path, "/path/to/data")

class TestApp(unittest.TestCase):
    """docstring for TestApp"""
    def setUp(self):
        #setUp before each test
        self.app = App('test/test_app')
        self.app.new()
        self.app.load()

    def tearDown(self):
        # tearDown after each test
        pass
        # shutil.rmtree('test/test_app')

    def test_load(self):
        self.assertEqual(self.app.config['app']['name'], 'app name')
        # print app.config

    def test_new(self):
        #done in setUP
        pass

    def test_setParameters(self):
        self.app.parameters['inputs']['bam']={'data': [{"name":"/path/to/data1"}, {"name":"/path/to/data2"}]}
        self.app.setParameters()
        self.assertEqual(self.app.config['app']['inputs']['bam'][0].path, "/path/to/data1")
        self.assertEqual(self.app.config['app']['outputs']['results'][0].path[0:10], "/var/data/")
        self.assertEqual(self.app.config['app']['outputs']['results'][0].path[-3:], 'tgz')
        #try setParameters ater newParameters
        self.app.newParameters('test/test_app/test_parameter.yaml')
        self.app.parameters['parameters']['workspace']['value'] = '/path/to/data3'
        self.app.setParameters()
        self.assertEqual(self.app.config['app']['parameters']['workspace'].__str__(), '/path/to/data3')

    def test_newParameters(self):
        self.app.newParameters('test/test_app/test_parameter.yaml')
        # print self.app.parameters

    def test_newParameters_after_setParameters(self):
        self.app.setParameters()
        self.app.newParameters('test/test_app/test_parameter.yaml')

    def test_newParameters_before_setParameters(self):
        self.app.newParameters('test/test_app/test_parameter.yaml')
        self.app.setParameters()

    def test_newParameters_and_setParameters_more(self):
        self.app.newParameters('test/test_app/test_parameter.yaml')
        self.app.setParameters()
        self.app.newParameters('test/test_app/test_parameter.yaml')
        self.app.setParameters()

    def test_newParameters_and_setParameters_more2(self):
        self.app.setParameters()
        self.app.newParameters('test/test_app/test_parameter.yaml')
        self.app.setParameters()
        self.app.newParameters('test/test_app/test_parameter.yaml')

    def test_loadParameters(self):
        self.app.newParameters('test/test_app/test_parameter.yaml')
        self.app.loadParameters('test/test_app/test_parameter.yaml')

    def test_workflow(self):
        self.app.buildTestWorkflow()
        # print self.app.workflow

if __name__ == '__main__':
    unittest.main()
