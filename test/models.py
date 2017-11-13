from core.models import Base, Project, Module, App, Task, Bcs, Instance
from core.models import CREATED
from core.models import BCS
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import unittest
import shutil
import pdb
import datetime
import os
import getpass

engine = create_engine('sqlite:///:memory:', echo=False)
Base.metadata.create_all(engine)

class TestProject(unittest.TestCase):
    """docstring for TestProject"""
    def setUp(self):
	self.Session = sessionmaker(bind=engine)
	self.session = self.Session()

    def tearDown(self):
	map(self.session.delete, self.session.query(Project).all())
	self.session.flush()
        self.session.close()

    def test_add(self):
	proj = Project(id='proj-id', name='test')
	self.session.add(proj)
	self.session.commit()

    def test_query(self):
        proj = self.session.query(Project).first()
	self.assertEqual(str(proj), "<Project(id=proj-id, name=test)>")
	self.assertEqual(proj.owner, getpass.getuser())
	self.assertEqual(proj.type, BCS)
	self.assertEqual(proj.status, CREATED)
	self.assertEqual(proj.start_date, None)
	self.assertEqual(len(proj.task), 0)

class TestModule(unittest.TestCase):
    """docstring for TestModule"""
    def setUp(self):
	self.Session = sessionmaker(bind=engine)
	self.session = self.Session()

    def tearDown(self):
	map(self.session.delete, self.session.query(Module).all())
	self.session.flush()
        self.session.close()

    def test_add(self):
        module = Module(name = 'Filter_rRNA', alias='Filter')
	self.session.add(module)
	self.session.commit()

    def test_query(self):
        module = self.session.query(Module).first()
	self.assertEqual(str(module), "<Module(id=1, name=Filter_rRNA)>")
	self.assertEqual(module.alias, "Filter")

class TestApp(unittest.TestCase):
    """docstring for TestModule"""
    def setUp(self):
	self.Session = sessionmaker(bind=engine)
	self.session = self.Session()

    def tearDown(self):
        self.session.close()

    def test_add(self):
        module = Module(name = 'Filter_rRNA', alias='Filter')
        app1 = App(name = 'RMrRNA_SOAP2', cpu=4, mem=8)
        app2 = App(name = 'RMrRNA_Statistic', cpu=1, mem=2)
	app1.module = module
	module.app.append(app2)
	self.session.add_all([app1, app2])
	self.session.commit()

    def test_query(self):
        module = self.session.query(Module).first()
        app = self.session.query(App).first()
	self.assertEqual(app.name, "RMrRNA_SOAP2")
	self.assertEqual(app.module.alias, "Filter")
	self.assertEqual(len(module.app), 2)
	self.assertEqual(module.app[1].name, "RMrRNA_Statistic")

if __name__ == '__main__':
    unittest.main()
