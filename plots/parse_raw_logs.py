from distutils.util import subst_vars
import json
import re
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pprint import pprint

# INPUT_FILES = ['geospatial/ray_/naive_tasks_cgs_quarter_job_logs.txt']
INPUT_FILES = ['geospatial/ray_/naive_quarter_zeroworkers.txt']


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


if __name__ == '__main__':
    logs = []
    for input_file in INPUT_FILES:
        with open(input_file, 'r') as file:
            logs.extend(file.readlines())

    task_events = []
    ray_events = []
    for log in logs:
        if '>>>' in log:
            event = parse_entry(log)
            task_events.append(event)
        if '(scheduler' in log:
            event = parse_ray_scheduler_entry(log)
            print(event)
    
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
    times_X = [int(t1)-t for t in times][::-1]
    # print(times_X)

    running_tasks_X = []
    for time in times:
        running_tasks = 0
        for t0, t1 in tasks.values():
            if time >= t0 and time <= t1:
                running_tasks += 1
        running_tasks_X.append(running_tasks)
    
    avail_cpus = np.zeros(len(times_X))
    for evt, t, val in ray_events:
        if evt == 'resized':
            

    for time in times:
        cpus = 0
        for t0, t1 in tasks.values():
            if time >= t0 and time <= t1:
                running_tasks += 1
        running_tasks_X.append(running_tasks)

    # pprint(running_tasks_X)
    fig, ax1 = plt.subplots()

    ax1.plot(times_X, running_tasks_X)
    plt.axvline(x = 7, color = 'b', label = 'axvline - full height')
    # ax.plot(times_X, pods_b2)

    # ax1.set_xlabel('Wallclock time (s)')

    # ax1.plot(times_X, pods_b1, lw=1.5, ls='--', label='Running Pods\nfor bucket-1')
    # ax1.fill_between(times_X, np.min(pods_b1), pods_b1, alpha=0.25)
    # ax1.plot(times_X, pods_b2, lw=1.5, ls='--', label='Running Pods\nfor bucket-2')
    # ax1.fill_between(times_X, np.min(pods_b2), pods_b2, alpha=0.25)
    # ax1.set_yticks(np.arange(1, 10, 3))
    # ax1.set_ylabel('No. Pods')

    # ax2 = ax1.twinx()
    # ax2.plot(times_X, reqs_b1, lw=2.5, label='Requests-in-flight\nfor bucket-1')
    # ax2.plot(times_X, reqs_b2, lw=2.5, label='Requests-in-flight\nfor bucket-2')
    # ax2.set_yticks(np.arange(1, 100, 10))
    # ax2.set_ylabel('No. Requests in flight')

    # colors = ['pink', 'lightblue', 'lightgreen']
    # for bplot in (bp1, bp2):
    #     for patch, color in zip(bplot['boxes'], colors):
    #         patch.set_facecolor(color)

    # fig.set_size_inches(10, 8)
    # fig.suptitle('No-op PUT/GET 100 MiB payload')
    fig.tight_layout()
    # handles, labels = handles, labels = [(a + b) for a, b in zip(ax1.get_legend_handles_labels(), ax2.get_legend_handles_labels())]
    # plt.legend(handles, labels, loc='upper left',)
    # # plt.show()
    #fig.savefig(f'plot.pdf')
    fig.savefig(f'plot.png', dpi=300)
