import unittest
from core.app import App
import shutil

class TestApp(unittest.TestCase):
    """docstring for TestApp"""
    # def setUp(self):
    #     pass
        #self.app = App('load_app')

    def test_load(self):
        app = App('test/load_app')
        app.load()
        print app.config

    def test_new(self):
        app = App('test/new_app')
        app.new()
        shutil.rmtree('test/new_app')
        print app.config

if __name__ == '__main__':
    unittest.main()
