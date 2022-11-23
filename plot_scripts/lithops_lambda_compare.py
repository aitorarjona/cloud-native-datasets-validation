import json
import os
import uuid
import math
import re
from types import SimpleNamespace
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pprint import pprint

matplotlib.rc('image', cmap='gray')
# matplotlib.style.use('seaborn-white')
matplotlib.rc('font', size=10.5)
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

logs1 = ['geospatial/ray_/naive_quarter_zeroworkers.txt']
logs2 = ['geospatial/ray_/co_quarter_zeroworkers.txt']

INITIAL_CPUS = 4


def parse_entry(line, group_id=None):
    group_id = group_id or '0'

    _, log = line.split('>>>', 1)
    log = log.strip()
    splits = log.split(' - ')

    if len(splits) == 5:
        splits = splits[1::]

    entry = {
        'task_id': group_id,
        'stage': splits[0].strip(),
        'event': splits[1].strip(),
        't': float(splits[2].strip()),
        'other': [s.strip() for s in splits[3:]]
    }
    return entry


def parse_ray_scheduler_entry(line):
    m = re.findall(r'\(scheduler\s\+\d+s\)', line)
    if m:
        sub = re.findall(r'Resized\sto\s\d+\sCPUs', line)
        if sub:
            time_stamp = m.pop()
            matches = re.findall(r'\d+', time_stamp)
            t = math.ceil(matches[0]), math.ceil(matches[1])

            log = sub.pop()
            matches = re.findall(r'\d+', log)
            cpus = math.ceil(matches.pop())

            return 'resized', t, cpus
        
        sub = re.findall(r'Adding\s\d+\snodes\sof\stype', line)
        if sub:
            time_stamp = m.pop()
            matches = re.findall(r'\d+', time_stamp)
            t = math.ceil(matches.pop())

            log = sub.pop()
            matches = re.findall(r'\d+', log)
            nodes = math.ceil(matches.pop())

            return 'scale_up', t, nodes

    
    m = re.findall(r'\(scheduler\s\+\d+m\d+s\)', line)
    if m:
        sub = re.findall(r'Resized\sto\s\d+\sCPUs', line)
        if sub:
            time_stamp = m.pop()
            matches = re.findall(r'\d+', time_stamp)
            min, sec = math.ceil(matches[0]), math.ceil(matches[1])
            t = (min * 60) + sec

            log = sub.pop()
            matches = re.findall(r'\d+', log)
            cpus = math.ceil(matches.pop())

            return 'resized', t, cpus
        
        sub = re.findall(r'Adding\s\d+\snodes\sof\stype', line)
        if sub:
            time_stamp = m.pop()
            matches = re.findall(r'\d+', time_stamp)
            min, sec = math.ceil(matches[0]), math.ceil(matches[1])
            t = (min * 60) + sec

            log = sub.pop()
            matches = re.findall(r'\d+', log)
            nodes = math.ceil(matches.pop())

            return 'scale_up', t, nodes


def lithops_parse_logs(func_logs, stats_logs, orch_logs=None):
    current_group = []
    groups = []
    
    for line in func_logs:
        line = line.strip()
        current_group.append(line)

        if len(line) == 1 and line[0] == ']':
            groups.append(current_group)
            current_group = []
    
    task_events = []
    for group in groups:
        group_id = uuid.uuid4().hex
        for line in group:
            if '>>>' in line:
                event = parse_entry(line, group_id)
                task_events.append(event)
    
    tasks = {}
    for event in task_events:
        if event['event'] == 'start':
            if event['task_id'] in tasks:
                t0, t1 = tasks[event['task_id']]
                tasks[event['task_id']] = (event['t'], t1)
                pass
            else:
                tasks[event['task_id']] = (event['t'], 0)
        elif event['event'] == 'end':
            if event['task_id'] in tasks:
                t0, t1 = tasks[event['task_id']]
                tasks[event['task_id']] = (t0, event['t'])
                pass
            else:
                tasks[event['task_id']] = (0, event['t'])
    
    if orch_logs:
        t0, t1 = None, None
        for log in logs:
            if '>>> pipeline - start' in orch_logs:
                entry = parse_entry(log)
                t0 = entry['t']
            elif '>>> pipeline - end' in orch_logs:
                entry = parse_entry(log)
                t1 = entry['t']
    else:
        mins = []
        for stats_log in stats_logs:
            t = min([t['host_job_create_tstamp'] for t in stats_log])
            mins.append(t)
        t0 = min(mins)

        maxs = []
        for stats_log in stats_logs:
            t = min([t['host_status_done_tstamp'] for t in stats_log])
            maxs.append(t)
        t1 = max(maxs)
    
    times = np.arange(math.ceil(t0), math.ceil(t1), 1)
    times_X = np.array([math.ceil(t1)-t for t in times][::-1])

    running_tasks_X = np.zeros(len(times_X))
    for i, time in enumerate(times):
        running_tasks = 0
        for tt0, tt1 in tasks.values():
            if time >= tt0 and time <= tt1:
                running_tasks += 1
        running_tasks_X[i] = running_tasks
    
    avail_cpus_X = np.zeros(len(times_X))
    for i, time in enumerate(times):
        running_tasks = 0
        for stats_log in stats_logs:
            for stat in stats_log:
                tw0 = stat['worker_start_tstamp'] 
                tw1 = stat['worker_end_tstamp']
                if time >= tw0 and time <= tw1:
                    running_tasks += 1
        avail_cpus_X[i] = running_tasks
    
    scaleup_events = []
    # for stats_log in stats_logs:
    #     scaleup_t = min(t['host_submit_tstamp'] for t in stats_log)
    #     scaleup_events.append(scaleup_t - t0)
    
    result = SimpleNamespace()
    result.t0 = t0
    result.t1 = t1
    result.total = t1 - t0
    result.times_X = times_X
    result.running_tasks_X = running_tasks_X
    result.avail_cpus_X = avail_cpus_X
    result.scaleup_events = scaleup_events
    return result


if __name__ == '__main__':
    naive_dir = 'geospatial/lithops_/naive_lithops_quarter/'

    naive_logs = []
    naive_stats = []
    for filename in os.listdir(naive_dir):
        with open(os.path.join(naive_dir, filename), 'r') as file:
            if filename.endswith('.log'):
                logs = file.readlines()
                naive_logs.extend(logs)
            elif filename.endswith('.json'):
                stats = json.loads(file.read())
                naive_stats.append(stats)
    
    naive_res = lithops_parse_logs(naive_logs, naive_stats)

    print(f'Total naive time: {naive_res.total}')

    co_dir = 'geospatial/lithops_/co_lithops_quarter/'

    co_logs = []
    co_stats = []
    for filename in os.listdir(co_dir):
        with open(os.path.join(co_dir, filename), 'r') as file:
            if filename.endswith('.log'):
                logs = file.readlines()
                co_logs.extend(logs)
            elif filename.endswith('.json'):
                stats = json.loads(file.read())
                co_stats.append(stats)
    
    co_res = lithops_parse_logs(co_logs, co_stats)

    print(f'Total co time: {co_res.total}')

    print(f'Percent diff co/naive is {(naive_res.total * 100) / co_res.total}')

    # pprmath.ceil(running_tasks_X)
    # sns.set_style("white")
    # sns.set_theme()

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, sharey=True)

    ax1a = ax1
    ax1a.plot(naive_res.times_X, naive_res.running_tasks_X, c='tab:blue')
    ax1a.set_ylabel('Running tasks', c='tab:blue')
    ax1a.tick_params(axis='y', colors='tab:blue')

    # ax1a.set_xlabel('Wallclock time (s)')
    ax1a.grid(alpha=0.5, axis='y')

    ax1b = ax1a.twinx()

    ax1b.plot(naive_res.times_X, naive_res.avail_cpus_X, c='tab:orange', ls=':')
    ax1b.set_ylabel('Available CPUs', c='tab:orange')
    ax1b.tick_params(axis='y', colors='tab:orange')

    for i, scaleup_event in enumerate(naive_res.scaleup_events):
        if i == 0:
            ax1a.axvline(x=scaleup_event, c='tab:green', ls='--', alpha=0.75, label='Scale-up event')
        else:
            ax1a.axvline(x=scaleup_event, c='tab:green', ls='--', alpha=0.75)

    ax1b.grid(alpha=0.5, axis='y', ls=':')

    handles, labels = ax1.get_legend_handles_labels()
    if handles and labels:
        ax1.legend(handles, labels)

    #####

    ax2a = ax2
    ax2a.plot(co_res.times_X, co_res.running_tasks_X, c='tab:blue')
    ax2a.set_ylabel('Running tasks', c='tab:blue')
    ax2a.tick_params(axis='y', colors='tab:blue')

    ax2a.set_xlabel('Wallclock time (s)')
    ax2a.grid(alpha=0.5, axis='y')

    ax2b = ax2a.twinx()

    ax2b.plot(co_res.times_X, co_res.avail_cpus_X, c='tab:orange', ls=':')
    ax2b.set_ylabel('Requested CPUs', c='tab:orange')
    ax2b.tick_params(axis='y', colors='tab:orange')

    for i, scaleup_event in enumerate(co_res.scaleup_events):
        if i == 0:
            ax2a.axvline(x=scaleup_event, c='tab:green', ls='--', alpha=0.75, label='Scale-up event')
        else:
            ax2a.axvline(x=scaleup_event, c='tab:green', ls='--', alpha=0.75)

    ax2b.grid(alpha=0.5, axis='y', ls=':')

    handles, labels = ax2.get_legend_handles_labels()
    if handles and labels:
        ax2.legend(handles, labels)

    fig.tight_layout()
    fig.savefig(f'lithops_compare.png', dpi=300)
