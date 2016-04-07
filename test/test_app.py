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
        print output.path

    def test_enid(self):
        output = AppFile(self.file)
        print output.enid

    def test_format(self):
        self.file['formats'] = 'bam'
        output = AppFile(self.file)
        print output.path

    def test_name(self):
        self.file['name'] = '/path/to/data'
        output = AppFile(self.file)
        print output.path

class TestApp(unittest.TestCase):
    """docstring for TestApp"""
    def setUp(self):
        print "=====setUP====="
        self.app = App('test/test_app')
        self.app.new()

    def tearDown(self):
        print "=====tearDown====="
        shutil.rmtree('test/test_app')

    def test_load(self):
        print "=====load====="
        app = App('test/test_app')
        app.load()
        # print app.config

    def test_new(self):
        print "=====new====="
        # print self.app.config

    def test_setParameters(self):
        print "=====set_default_parameters====="
        self.app.load()
        # print self.app.config['app']['parameters']['workspace']

        self.app.parameters['inputs']['bam']={'data': [{"name":"/fwefwe/fwef/wef"}, {"name":"/fwe/fwe/2"}]}
        self.app.setParameters()
        print self.app.config['app']['inputs']['bam']
        print self.app.config['app']['inputs']['bam'][0].path
        print self.app.config['app']['outputs']['results'][0].path
        print self.app.config['app']['outputs']['results']
        print self.app.config

if __name__ == '__main__':
    unittest.main()
