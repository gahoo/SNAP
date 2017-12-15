from core.colorMessage import dyeWARNING, dyeFAIL, dyeOKGREEN, dyeOKBLUE
from prettytable import PrettyTable
import os

def format_project_tbl(projects):
    def build_row(name, state):
        progress = 100.0 * state.get('finished', 0) / sum(state.values())
        return [name] + [state.get(column, 0) for column in states_column] + [round(progress, 2)]

    tbl = PrettyTable()
    states = {e.name:e.states() for e in projects}
    states_column = sum([state.keys() for state in states.values()], [])
    tbl.field_names = ['project'] + states_column + ['progress(%)']
    for name, state in states.items():
        row = build_row(name, state)
        tbl.add_row(row)
    return tbl

def format_tasks_tbl(tasks):
    tbl = PrettyTable()
    tbl.field_names = ['id', 'name', 'status', 'failed', 'module', 'app', 'instance', 'create', 'waited', 'elapsed']
    for task in tasks:
        failed_cnts = len([b for b in task.bcs if b.status == 'Failed'])
        create_date = get_date(task.bcs[-1].create_date)
        start_date = get_date(task.bcs[-1].start_date)
        finish_date = get_date(task.bcs[-1].finish_date)
        waited = diff_date(create_date, start_date)
        elapsed = diff_date(start_date, finish_date)
        status = format_status(task.aasm_state, task.aasm_state == 'cleaned')
        row = [task.id, os.path.basename(task.shell), status, failed_cnts, task.module.name, task.app.name, task.instance.name, create_date, waited, elapsed]
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
    tbl.field_names = ['name', 'source', 'destination', 'is_write', 'is_immediate']
    for m in mappings:
        row = [m.name, m.source, m.destination, m.is_write, m.is_immediate]
        tbl.add_row(row)

    return tbl

