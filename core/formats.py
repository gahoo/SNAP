from core.colorMessage import dyeWARNING, dyeFAIL, dyeOKGREEN, dyeOKBLUE
from prettytable import PrettyTable
import os
import datetime
import functools
import pdb

def build_list_if(boolean, func):
    if boolean:
        return func()
    else:
        return []


def format_project_tbl(projects, size=False, cost=False):
    def get_size():
        row_size = p.size_stat()
        row_size = [row_size[k] for k in ('clean', 'project')]
        return row_size

    build_size_field = functools.partial(build_list_if, boolean = size, func = lambda: ['clean Data', 'project data'])
    build_size = functools.partial(build_list_if, boolean = size, func = get_size)
    build_cost_field = functools.partial(build_list_if, boolean = cost, func = lambda: ['cost'])
    build_cost = functools.partial(build_list_if, boolean = cost, func = lambda: [p.cost()])

    def build_row(p):
        state = states[p.name]
        progress = 100.0 * (state.get('cleaned', 0) + state.get('finished', 0)) / sum(state.values())
        progress = round(progress, 2)
        elapsed = diff_date(get_date(p.start_date), get_date(p.finish_date))
        row_size = build_size()
        row_cost = build_cost()
        if size:
            row_cost = [sum(row_size) * 0.148 + row_cost[0]]
        return [p.name] + [state.get(column, 0) for column in states_column] + [progress, elapsed] + row_size + row_cost

    tbl = PrettyTable()
    states = {e.name:e.states() for e in projects}
    states_column = sum([state.keys() for state in states.values()], [])
    size_field = build_size_field()
    cost_field = build_cost_field()
    tbl.field_names = ['project'] + states_column + ['progress(%)', 'elapsed'] + size_field + cost_field

    for p in projects:
        row = build_row(p)
        tbl.add_row(row)
    return tbl

def format_detail_porject(project):
    tbl = PrettyTable()
    tbl.header = False
    fields = ['id', 'name', 'description', 'owner', 'status', 'type', 'pipe', 'path', 'max_job', 'run_cnt', 'create_date', 'start_date', 'finish_date', 'discount', 'email', 'mns', 'cluster']
    values = [project.__getattribute__(k) for k in fields]
    elapsed = diff_date(get_date(project.start_date), get_date(project.finish_date))
    tbl.add_column("field", fields + ['task num', 'elapsed'])
    tbl.add_column("value", values + [len(project.task), elapsed])
    return tbl

def format_tasks_tbl(tasks, cost=False):
    build_cost_field = functools.partial(build_list_if, boolean = cost, func = lambda: ['cost'])
    build_cost = functools.partial(build_list_if, boolean = cost, func = lambda: [task.cost()])

    tbl = PrettyTable()
    cost_field = build_cost_field()
    tbl.field_names = ['id', 'name', 'status', 'failed', 'module', 'app', 'instance', 'create', 'waited', 'elapsed'] + cost_field
    for task in tasks:
        if task.bcs:
            failed_cnts = len([b for b in task.bcs if b.status == 'Failed'])
            create_date = get_date(task.bcs[-1].create_date)
            start_date = get_date(task.bcs[-1].start_date)
            finish_date = get_date(task.bcs[-1].finish_date)
            waited = diff_date(create_date, start_date)
            elapsed = diff_date(start_date, finish_date)
        else:
            failed_cnts = 0
            create_date = task.project.create_date.replace(microsecond=0)
            waited = elapsed = None
        cost = build_cost()
        status = format_status(task.aasm_state, task.aasm_state == 'cleaned')
        row = [task.id, os.path.basename(task.shell), status, failed_cnts, task.module.name, task.app.name, task.instance.name, create_date, waited, elapsed] + cost
        tbl.add_row(row)
    return tbl

def get_date(date):
    if date:
        return date.replace(microsecond=0)
    elif date is None:
        return None
    else:
        raise ValueError("Invalid datetime")

def diff_date(t1, t2):
    if all([t1, t2]):
        return t2 - t1
    elif t1 and not t2:
        now = datetime.datetime.now().replace(microsecond=0)
        return now - t1
    else:
        return None

def format_detail_task(task):
    tbl = PrettyTable()
    tbl.header = False
    fields = ['id', 'name', 'status', 'module', 'app', 'instance', 'cpu', 'mem', 'disk_type', 'disk_size']
    values = [task.id, os.path.basename(task.shell), task.aasm_state, task.module.name, task.app.name, task.instance.name, task.cpu, task.mem, task.disk_type, task.disk_size]
    tbl.add_column("field", fields)
    tbl.add_column("value", values)
    return tbl

def format_bcs_tbl(bcs, with_instance):
    tbl = PrettyTable()
    fields = ['id', 'name', 'status', 'create', 'waited', 'elapsed']
    if with_instance:
        fields = fields + ['instance', 'cpu', 'mem', 'instance price', 'spot price']
    tbl.field_names = fields
    for b in bcs:
        status = format_status(b.status.lower(), b.deleted)
        create_date = get_date(b.create_date)
        start_date = get_date(b.start_date)
        finish_date = get_date(b.finish_date)
        waited = diff_date(create_date, start_date)
        elapsed = diff_date(start_date, finish_date)
        row = [b.id, b.name, status, create_date, waited, elapsed]
        if with_instance:
            i = b.instance
            row = row + [i.name, i.cpu, i.mem, i.price, b.spot_price_limit]
        tbl.add_row(row)
    return tbl

def format_status(status, deleted):
    if status == 'failed':
        status = dyeFAIL(status)
    elif status == 'finished':
        status = dyeOKGREEN(status)
    elif status == 'stopped':
        status = dyeWARNING(status)
    else:
        status = dyeOKBLUE(status)

    if deleted:
        status = status + " (D)"
    return status

def format_mapping_tbl(mappings):
    tbl = PrettyTable()
    tbl.field_names = ['id', 'name', 'source', 'destination', 'is_write', 'is_immediate']
    for m in mappings:
        if m.exists():
            destination = dyeOKGREEN(m.destination)
        else:
            destination = dyeFAIL(m.destination)
        row = [m.id, m.name, m.source, destination, m.is_write, m.is_immediate]
        tbl.add_row(row)

    return tbl

def format_instance_tbl(instances):
    tbl = PrettyTable()
    tbl.field_names = ['name', 'cpu', 'mem', 'disk type', 'disk size', 'price']
    for i in instances:
        row = [i.name, i.cpu, i.mem, i.disk_type, i.disk_size, i.price]
        tbl.add_row(row)

    return tbl
