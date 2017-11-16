from core.models import Base, Project, Module, App, Task, Bcs, Instance
from core.models import CREATED, PENDING
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
import pdb

engine = create_engine('sqlite:///:memory:', echo=False)
Base.metadata.create_all(engine)

class TestProject(unittest.TestCase):
    """docstring for TestProject"""
    def setUp(self):
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()

    def tearDown(self):
        self.session.query(Project).delete()
        self.session.flush()
        self.session.close()

    def test_add_project(self):
        proj = Project(id='proj-1', name='test')
        self.session.add(proj)
        self.session.commit()

    def test_query_project(self):
        proj = self.session.query(Project).filter(Project.id == 'proj-1').first()
        self.assertEqual(str(proj), "<Project(id=proj-1, name=test)>")
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
        self.session.query(Module).delete()
        self.session.flush()
        self.session.close()

    def test_add_module(self):
        module = Module(name = 'Filter_rRNA', alias='Filter')
        self.session.add(module)
        self.session.commit()

    def test_query_module(self):
        module = self.session.query(Module).first()
        self.assertEqual(str(module), "<Module(id=1, name=Filter_rRNA)>")
        self.assertEqual(module.alias, "Filter")

class TestApp(unittest.TestCase):
    """docstring for TestModule"""
    def setUp(self):
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()

    def tearDown(self):
        self.session.query(Module).delete()
        self.session.query(App).delete()
        self.session.flush()
        self.session.close()

    def test_add_app(self):
        module = Module(name = 'Filter_rRNA', alias='Filter')
        app1 = App(name = 'RMrRNA_SOAP2', cpu=4, mem=8)
        app2 = App(name = 'RMrRNA_Statistic', cpu=1, mem=2)
        app1.module = module
        module.app.append(app2)
        self.session.add_all([app1, app2])
        self.session.commit()

    def test_query_app(self):
        module = self.session.query(Module).first()
        app = self.session.query(App).first()
        self.assertEqual(app.name, "RMrRNA_SOAP2")
        self.assertEqual(app.module.alias, "Filter")
        self.assertEqual(len(module.app), 2)
        self.assertEqual(module.app[1].name, "RMrRNA_Statistic")

class TestTask(unittest.TestCase):
    """docstring for TestModule"""
    def setUp(self):
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()

    def tearDown(self):
        self.session.query(Project).delete()
        self.session.query(Module).delete()
        self.session.query(App).delete()
        self.session.query(Task).delete()
        self.session.flush()
        self.session.close()

    def test_add_task(self):
        proj = Project(id='proj-2', name='test')
        module = Module(name = 'Filter_rRNA', alias='Filter')
        app1 = App(name = 'RMrRNA_SOAP2', cpu=4, mem=8)
        task1 = Task(shell='test1.sh', module=module, app=app1, project=proj)
        task2 = Task(shell='test2.sh', module=module, app=app1, project=proj)
        task2.dependence.append(task1)
        self.session.add_all([module, app1, task1, task2])
        self.session.commit()

    def test_query_task(self):
        task = self.session.query(Task).filter(Task.shell=='test2.sh').first()
        self.assertEqual(task.dependence[0].shell, 'test1.sh')
        self.assertEqual(task.shell, 'test2.sh')
        self.assertEqual(task.app.name, 'RMrRNA_SOAP2')
        self.assertEqual(task.module.name, 'Filter_rRNA')

class TestBcs(unittest.TestCase):
    """docstring for TestModule"""
    def setUp(self):
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()

    def tearDown(self):
        self.session.query(Project).delete()
        self.session.query(Module).delete()
        self.session.query(App).delete()
        self.session.query(Task).delete()
        self.session.query(Bcs).delete()
        self.session.flush()
        self.session.close()

    def test_add_bcs(self):
        proj = Project(id='proj-3', name='test')
        module = Module(name = 'Filter_rRNA', alias='Filter')
        app = App(name = 'RMrRNA_SOAP2', cpu=4, mem=8)
        task = Task(shell='test3.sh', module=module, app=app, project=proj)
        bcs1 = Bcs(id='job-1', task=task)
        bcs2 = Bcs(id='job-2', task=task)
        self.session.add_all([bcs1, bcs2])
        self.session.commit()

    def test_query_bcs(self):
        bcs = self.session.query(Bcs).all()
        self.assertEqual(bcs[0].id, 'job-1')
        self.assertEqual(bcs[0].task.shell, 'test3.sh')
        self.assertEqual(bcs[0].task.module.name, 'Filter_rRNA')
        self.assertEqual(bcs[0].task.app.name, 'RMrRNA_SOAP2')
        self.assertEqual(bcs[0].task.bcs[0], bcs[0])

class TestInstance(unittest.TestCase):
    """docstring for TestModule"""
    def setUp(self):
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()

    def tearDown(self):
        self.session.query(Project).delete()
        self.session.query(Module).delete()
        self.session.query(App).delete()
        self.session.query(Task).delete()
        self.session.query(Bcs).delete()
        self.session.flush()
        self.session.close()

    def test_add_instance(self):
        proj = Project(id='proj-4', name='test')
        instance = Instance(name='bcs.a2.large', cpu=4, mem=8, price=0.4)
        app = App(name = 'RMrRNA_Statistic', cpu=4, mem=8, instance=instance)
        task = Task(shell='test4.sh', project=proj, instance=instance)
        bcs = Bcs(id='job-3', task=task, instance=instance)
        self.session.add(instance)
        self.session.commit()

    def test_query_instance(self):
        instance = self.session.query(Instance).first()
        app = self.session.query(App).filter(App.name == 'RMrRNA_Statistic').first()
        task = self.session.query(Task).filter(Task.shell == 'test4.sh').first()
        bcs = self.session.query(Bcs).filter(Bcs.id == 'job-3').first()
        self.assertTrue(app.instance is instance)
        self.assertTrue(bcs.instance is instance)
        self.assertTrue(task.instance is instance)


if __name__ == '__main__':
    unittest.main()
