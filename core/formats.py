from core.colorMessage import dyeWARNING, dyeFAIL, dyeOKGREEN, dyeOKBLUE
from core.misc import human_size
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
        row_size = build_size()
        row_cost = build_cost()
        if size and row_cost:
            row_cost = [round(sum(row_size) / (2.0 ** 30) * 0.148, 3) + row_cost[0]]
        row_size = map(human_size, row_size)
        return [p.name] + [state.get(column, 0) for column in states_column] + [progress, p.elapsed()] + row_size + row_cost

    tbl = PrettyTable()
    states = {e.name:e.states() for e in projects}
    states_column = set(sum([state.keys() for state in states.values()], []))
    size_field = build_size_field()
    cost_field = build_cost_field()
    tbl.field_names = ['project'] + list(states_column) + ['progress(%)', 'elapsed'] + size_field + cost_field

    for p in projects:
        row = build_row(p)
        tbl.add_row(row)
    return tbl

def format_detail_porject(project):
    tbl = PrettyTable()
    tbl.header = False
    fields = ['id', 'name', 'description', 'owner', 'status', 'type', 'pipe', 'path', 'max_job', 'run_cnt', 'create_date', 'start_date', 'finish_date', 'discount', 'email', 'mns', 'cluster', 'auto_scale']
    values = [project.__getattribute__(k) for k in fields]
    tbl.add_column("field", fields + ['task num', 'elapsed'])
    tbl.add_column("value", values + [len(project.task), project.elapsed()])
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
            waited = task.bcs[-1].waited()
            elapsed = task.bcs[-1].elapsed()
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
    fields = ['id', 'name', 'status', 'module', 'app', 'instance', 'docker_image', 'cpu', 'mem', 'disk_type', 'disk_size', 'debug_mode', 'benchmark']
    status = format_status(task.aasm_state, task.aasm_state == 'cleaned')
    values = [task.id, os.path.basename(task.shell), status, task.module.name, task.app.name, task.instance.name, task.docker_image, task.cpu, task.mem, task.disk_type, task.disk_size, task.debug_mode, task.benchmark]
    tbl.add_column("field", fields)
    tbl.add_column("value", values)
    return tbl

def format_bcs_tbl(bcs, with_instance):
    tbl = PrettyTable()
    fields = ['id', 'name', 'status', 'create', 'waited', 'elapsed', 'cost']
    if with_instance:
        fields = fields + ['instance', 'cpu', 'mem', 'instance price', 'spot price']
    tbl.field_names = fields
    for b in bcs:
        status = format_status(b.status.lower(), b.deleted)
        create_date = get_date(b.create_date)
        row = [b.id, b.name, status, create_date, b.waited(), b.elapsed(), b.cost]
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

def format_mapping_tbl(mappings, size=False):
    def dye_path(is_exists, path):
        if is_exists:
            return dyeOKGREEN(path)
        else:
            return dyeFAIL(path)

    tbl = PrettyTable()
    fields = ['id', 'name', 'source', '', 'destination', 'is_write', 'is_immediate', 'is_required']
    if size:
        fields += ['size']
    tbl.field_names = fields
    tbl.align['source'] = "l"
    tbl.align['destination'] = "l"
    tbl.align['size'] = "r"

    direction = {True: '<=', False: '=>'}

    for m in mappings:
        source = dye_path(os.path.exists(m.source), m.source)
        destination = dye_path(m.exists(), m.destination)
        row = [m.id, m.name, source, direction[m.is_write], destination, m.is_write, m.is_immediate, m.is_required]
        if size:
            row += [human_size(m.size())]
        tbl.add_row(row)

    return tbl

def format_detail_mapping(mapping, size=False):
    tbl = PrettyTable()
    tbl.header = False
    fields = ['id', 'name', 'source', 'destination', 'is_write', 'is_immediate', 'is_required']
    values = [mapping.__getattribute__(k) for k in fields]

    if mapping.exists():
        values[3] = dyeOKGREEN(values[3])
    else:
        values[3] = dyeFAIL(values[3])

    if size:
        fields += ['size']
        values += [human_size(mapping.size())]

    tbl.add_column("field", fields)
    tbl.add_column("value", values)
    return tbl

def format_instance_tbl(instances, latest_price = False):
    tbl = PrettyTable()
    fields = ['name', 'cpu', 'mem', 'disk type', 'disk size', 'price']
    if latest_price:
        fields += ['spot price', 'origin price', 'discount']
    tbl.field_names = fields
    for i in instances:
        row = [i.name, i.cpu, i.mem, i.disk_type, i.disk_size, i.price]
        if latest_price:
            (spot_price, origin_price) = i.latest_price()
            if origin_price:
                discount = spot_price / origin_price
            else:
                discount = None
            row += [spot_price, origin_price, discount]
        tbl.add_row(row)

    return tbl

def format_cost_tbl(costs):
    tbl = PrettyTable()
    tbl.field_names = ['id', 'name', 'size', 'size cost', 'bcs cost', 'total']
    map(tbl.add_row, costs)
    return tbl

def format_history_price_tbl(prices):
    tbl = PrettyTable()
    tbl.field_names = ['InstanceType', 'Timestamp', 'ZoneId', 'IoOptimized', 'NetworkType', 'OriginPrice', 'SpotPrice', 'Discount']
    for p in prices:
        p['Discount'] = round(p['SpotPrice'] / p['OriginPrice'], 2)
        tbl.add_row([p[key] for key in tbl.field_names])

    return tbl

def format_cluster_tbl(clusters, cluster_in_db=None):
    def buildDisk(disk, disk_type):
        return "[{disk_type}.{drive_type} {size}]".format(disk_type = disk_type, size = disk.Size, drive_type=disk.Type)

    def buildGroup(group):
        if group.Disks.DataDisk.Size:
            disks = group.Disks.DataDisk
            disk_type = 'Data'
        else:
            disks = group.Disks.SystemDisk
            disk_type = 'Sys'

        return "{actual}/{desired} | {instance} < {price} {disks}".format(
            instance = group.InstanceType,
            actual = group.ActualVMCount,
            desired = group.DesiredVMCount,
            disks = buildDisk(disks, disk_type),
            price = group.SpotPriceLimit)

    tbl = PrettyTable()
    tbl.field_names = ['id', 'name', 'state', 'cnt | instances < price [disk]', 'create time', 'elapsed']
    tbl.align['cnt | instances < price [disk]'] = "l"
    if cluster_in_db:
        clusters = [c for c in clusters if c.Id in cluster_in_db]
    for c in clusters:
        instances = map(buildGroup, c.Groups.values())
        instances = '\n'.join(instances)
        creation_time = c.CreationTime.replace(microsecond=0)
        tbl.add_row([c.Id, c.Name, c.State, instances, creation_time, diff_date(creation_time, None)])

    return tbl
