from distutils.util import subst_vars
import json
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


def parse_entry(line):
    _, log = line.split('>>>', 1)
    log = log.strip()
    splits = log.split(' - ')
    entry = {
        'task_id': splits[0].strip(),
        'stage': splits[1].strip(),
        'event': splits[2].strip(),
        't': float(splits[3].strip()),
        'other': [s.strip() for s in splits[4:]]
    }
    return entry


def parse_ray_scheduler_entry(line):
    m = re.findall(r'\(scheduler\s\+\d+s\)', line)
    if m:
        sub = re.findall(r'Resized\sto\s\d+\sCPUs', line)
        if sub:
            time_stamp = m.pop()
            matches = re.findall(r'\d+', time_stamp)
            t = int(matches[0]), int(matches[1])

            log = sub.pop()
            matches = re.findall(r'\d+', log)
            cpus = int(matches.pop())

            return 'resized', t, cpus
        
        sub = re.findall(r'Adding\s\d+\snodes\sof\stype', line)
        if sub:
            time_stamp = m.pop()
            matches = re.findall(r'\d+', time_stamp)
            t = int(matches.pop())

            log = sub.pop()
            matches = re.findall(r'\d+', log)
            nodes = int(matches.pop())

            return 'scale_up', t, nodes

    
    m = re.findall(r'\(scheduler\s\+\d+m\d+s\)', line)
    if m:
        sub = re.findall(r'Resized\sto\s\d+\sCPUs', line)
        if sub:
            time_stamp = m.pop()
            matches = re.findall(r'\d+', time_stamp)
            min, sec = int(matches[0]), int(matches[1])
            t = (min * 60) + sec

            log = sub.pop()
            matches = re.findall(r'\d+', log)
            cpus = int(matches.pop())

            return 'resized', t, cpus
        
        sub = re.findall(r'Adding\s\d+\snodes\sof\stype', line)
        if sub:
            time_stamp = m.pop()
            matches = re.findall(r'\d+', time_stamp)
            min, sec = int(matches[0]), int(matches[1])
            t = (min * 60) + sec

            log = sub.pop()
            matches = re.findall(r'\d+', log)
            nodes = int(matches.pop())

            return 'scale_up', t, nodes


def ray_parse_logs(logs):
    task_events = []
    ray_events = []
    for log in logs:
        if '>>>' in log:
            event = parse_entry(log)
            task_events.append(event)
        if '(scheduler' in log:
            event = parse_ray_scheduler_entry(log)
            if event:
                ray_events.append(event)
    
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
    
    t0, t1 = None, None
    for log in logs:
        if '>>> 0 - pipeline - start' in log:
            entry = parse_entry(log)
            t0 = entry['t']
        elif '>>> 0 - pipeline - end' in log:
            entry = parse_entry(log)
            t1 = entry['t']
    
    times = np.arange(int(t0), int(t1), 1)
    times_X = np.array([int(t1)-t for t in times][::-1])

    running_tasks_X = np.zeros(len(times_X))
    for i, time in enumerate(times):
        running_tasks = 0
        for t0, t1 in tasks.values():
            if time >= t0 and time <= t1:
                running_tasks += 1
        running_tasks_X[i] = running_tasks

    avail_cpus_X = np.array([INITIAL_CPUS] * len(times_X), dtype=np.int32)
    for evt, t, val in ray_events:
        if evt == 'resized':
            for i in range(len(times_X)):
                if i >= t:
                    avail_cpus_X[i] += val

    scaleup_events = []
    for evt, t, val in ray_events:
        if evt == 'scale_up':
            scaleup_events.append(t)
    
    result = SimpleNamespace()
    result.times_X = times_X
    result.running_tasks_X = running_tasks_X
    result.avail_cpus_X = avail_cpus_X
    result.scaleup_events = scaleup_events
    return result


if __name__ == '__main__':
    with open('geospatial/ray_/naive_quarter_zeroworkers.txt', 'r') as file:
        naive_logs = file.readlines()
    
    with open('geospatial/ray_/co_quarter_zeroworkers.txt', 'r') as file:
        co_logs = file.readlines()

    naive_res = ray_parse_logs(naive_logs)
    co_res = ray_parse_logs(co_logs)
     
    # pprint(running_tasks_X)
    # sns.set_style("white")
    # sns.set_theme()

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, sharey=True)

    ax1a = ax1
    ax1a.plot(naive_res.times_X, naive_res.running_tasks_X, c='tab:blue')
    ax1a.set_ylabel('Running tasks', c='tab:blue')
    ax1a.tick_params(axis='y', colors='tab:blue')

    ax1a.set_xlabel('Wallclock time (s)')
    ax1a.grid(alpha=0.5, axis='y')

    ax1b = ax1a.twinx()

    ax1b.plot(naive_res.times_X, naive_res.avail_cpus_X, c='tab:orange', ls='--')
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
        ax1.legend(handles, labels, loc='upper left')

    #####

    ax2a = ax2
    ax2a.plot(co_res.times_X, co_res.running_tasks_X, c='tab:blue')
    ax2a.set_ylabel('Running tasks', c='tab:blue')
    ax2a.tick_params(axis='y', colors='tab:blue')

    ax2a.set_xlabel('Wallclock time (s)')
    ax2a.grid(alpha=0.5, axis='y')

    ax2b = ax2a.twinx()

    ax2b.plot(co_res.times_X, co_res.avail_cpus_X, c='tab:orange', ls='--')
    ax2b.set_ylabel('Available CPUs', c='tab:orange')
    ax2b.tick_params(axis='y', colors='tab:orange')

    for i, scaleup_event in enumerate(co_res.scaleup_events):
        if i == 0:
            ax2a.axvline(x=scaleup_event, c='tab:green', ls='--', alpha=0.75, label='Scale-up event')
        else:
            ax2a.axvline(x=scaleup_event, c='tab:green', ls='--', alpha=0.75)

    ax2b.grid(alpha=0.5, axis='y', ls=':')

    handles, labels = ax2.get_legend_handles_labels()
    if handles and labels:
        ax2.legend(handles, labels, loc='upper left')

    fig.tight_layout()
    fig.savefig(f'ray_compare.png', dpi=300)
