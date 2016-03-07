import unittest
from core.app import App

class TestApp(unittest.TestCase):
    """docstring for TestApp"""
    def SetUp(self):
        self.app = App('test')

    def test_load(self):
        self.app.load('test/test_app.yaml')

if __name__ == '__main__':
    unittest.main()
