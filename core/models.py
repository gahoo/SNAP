from sqlalchemy import Column, Integer, Float, String, DateTime, ForeignKey, create_engine, Table
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
    description = Column(String)
    owner = Column(String, nullable=False)
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
    task = relationship("Task", back_populates="project")

    def __repr__(self):
        return "<Project(id={id}, name={name})>".format(id=self.id, name=self.name)

class Module(Base):
    __tablename__ = 'module'

    id = Column(String(20), primary_key=True)
    name = Column(String, nullable=False)
    alias = Column(String)
    yaml = Column(String)

    app = relationship("App", back_populates="module")
    task = relationship("Task", back_populates="module")

    def __repr__(self):
        return "<Module(id={id}, name={name})>".format(id=self.id, name=self.name)

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

    module = relationship("Module", back_populates="app")
    task = relationship("Task", back_populates="app")

    def __repr__(self):
        return "<App(id={id}, name={name})>".format(id=self.id, name=self.name)

dependence_table = Table('dependence', Base.metadata,
    Column('source_id', Integer, ForeignKey('task.id'), primary_key=True),
    Column('target_id', Integer, ForeignKey('task.id'), primary_key=True)
)

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

    project = relationship("Project", back_populates="task")
    module = relationship("Module", back_populates="task")
    app = relationship("App", back_populates="task")
    bcs = relationship("Bcs", back_populates="task")
    instance = relationship("Instance", back_populates="task")

    dependence = relationship("Task", secondary=dependence_table,
        primaryjoin=id==dependence_table.c.source_id,
        secondaryjoin=id==dependence_table.c.target_id,
        backref="depends")

    def __repr__(self):
        return "<Task(id={id} status={status})>".format(id=self.id, status=self.status)

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

    task = relationship("Task", back_populates="bcs")
    instance = relationship("Instance", back_populates="bcs")

    def __repr__(self):
        return "<Bcs(id={id} status={status})>".format(id=self.id, status=self.status)

class Instance(Base):
    __tablename__ = 'instance'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    cpu = Column(Integer)
    mem = Column(Integer)
    disk_type = Column(Integer)
    price = Column(Float)

    task = relationship("Task", back_populates="instance")
    bcs = relationship("Bcs", back_populates="instance")

    def __repr__(self):
        return "<Instance(id={id} cpu={cpu} mem={mem}, price={price})>".format(
	    id=self.id, cpu=self.cpu, mem=self.mem, price=self.price)
