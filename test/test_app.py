import unittest
from core.app import App
from core.app import AppParameter
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
        pass

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
        print app.config

    def test_new(self):
        print "=====new====="
        print self.app.config

    def test_set_default_parameters(self):
        print "=====set_default_parameters====="
        self.app.load()
        self.app.set_default_parameters()
        print self.app.parameters

if __name__ == '__main__':
    unittest.main()
