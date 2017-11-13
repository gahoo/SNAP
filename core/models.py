from sqlalchemy import Column, Integer, Float, String, DateTime, ForeignKey, create_engine
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

CREATED = 0
PENDING = 1

BCS = 0
SGE = 1
K8S = 2

CLOUD = 0
CLOUD_SSD = 1

class Project(Base):
    __tablename__ = 'project'

    id = Column(String(20), primary_key=True)
    name = Column(String(20), nullable=False)
    description = Column(String(20))
    owner = Column(String)
    status = Column(Integer, nullable=False)
    type = Column(Integer)
    pipe = Column(String)
    path = Column(String)
    max_job = Column(Integer)
    run_cnt = Column(Integer)
    create_date = Column(DateTime)
    start_date = Column(DateTime)
    finish_date = Column(DateTime)
    discount = Column(Float)
    email = Column(String(40))
    mns = Column(String)

class Module(Base):
    __tablename__ = 'module'

    id = Column(String(20), primary_key=True)
    name = Column(String, nullable=False)
    alias = Column(String)
    yaml = Column(String)

class App(Base):
    __tablename__ = 'app'

    id = Column(String(20), primary_key=True)
    name = Column(String, nullable=False)
    alias = Column(String)
    yaml = Column(String)
    cpu = Column(Integer)
    mem = Column(Float)
    disk_size = Column(Integer)
    disk_type = Column(Integer)
    module_id = Column(Integer, ForeignKey('module.id'))

    module = relationship("Module", back_populates="name")

class Task(Base):
    __tablename__ = 'task'
    
    id = Column(Integer, primary_key=True)
    shell = Column(String)
    status = Column(Integer)
    cpu = Column(Integer)
    mem = Column(Float)
    disk_size = Column(Integer)
    disk_type = Column(Integer)
    project_id = Column(Integer, ForeignKey('project.id'))
    module_id = Column(Integer, ForeignKey('module.id'))
    app_id = Column(Integer, ForeignKey('app.id'))
    instance_id = Column(Integer, ForeignKey('instance.id'))

    project = relationship("Project", back_populates="name")
    module = relationship("Module", back_populates="name")
    app = relationship("App", back_populates="name")
    instance = relationship("Instance", back_populates="name")

class Bcs(Base):
    __tablename__ = 'bcs'

    id = Column(String, primary_key=True)
    status = Column(Integer)
    spot_price = Column(Float)
    stdout = Column(String) # Path
    stderr = Column(String)
    create_date = Column(DateTime)
    start_date = Column(DateTime)
    finish_date = Column(DateTime)
    spot_price_limit = Column(Float)
    task_id = Column(Integer, ForeignKey('task.id'))
    instance_id = Column(Integer, ForeignKey('instance.id'))

    task = relationship("Task", back_populates="name")
    instance = relationship("Instance", back_populates="name")

class Dependence(Base):
    __tablename__ = 'dependence'

    source_id = Column(Integer, ForeignKey('task.id'), primary_key=True)
    #target_id = Column(Integer, ForeignKey('task.id'), primary_key=True)

    source = relationship("Task", back_populates="shell")
    #target = relationship("Task", back_populates="shell")

class Instance(Base):
    __tablename__ = 'instance'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    cpu = Column(Integer)
    mem = Column(Integer)
    disk_type = Column(Integer)
    price = Column(Float)
