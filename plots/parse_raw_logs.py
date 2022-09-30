import numpy as np
import re

# INPUT_FILES = ['geospatial/ray_/naive_tasks_cgs_quarter_job_logs.txt']
INPUT_FILES = ['geospatial/ray_/co_tasks_cgs_quarter_job_logs.txt']


def parse_entry(line):
    splits = line.split(' - ')
    stage = splits[0].replace('>>>', '').strip()
    event = splits[1].strip()
    t = float(splits[2].strip())
    other = [s.strip() for s in splits[3:]]
    return stage, event, t, other


def parse_ray_scheduler_entry(line):
    m = re.match(r'\+\d+m\d+s', line)
    print(m)


if __name__ == '__main__':
    logs = []
    for input_file in INPUT_FILES:
        with open(input_file, 'r') as file:
            logs.extend(file.readlines())

    # print(logs[2])

    events = []
    for log in logs:
        if '>>>' in log:
            event = parse_entry(log)
            events.append(event)
        elif '(scheduler' in log:  # parse ray scheduler logs
            event = parse_ray_scheduler_entry(log)
            events.append(event)

    t0, t1 = None, None
    for log in logs:
        if '>>> pipeline - start' in log:
            _, _, t, _ = parse_entry(log)
            t0 = t
        elif '>>> pipeline - end' in log:
            _, _, t, _ = parse_entry(log)
            t1 = t

    times = np.arange(int(t0), int(t1), 1)
    times_X = [int(t1)-t for t in times][::-1]
    print(times_X)

    tasks = []
    for t in times:
        n_tasks = 0
        for stage, event, t_event, args in events:
            if stage == 'pipeline':
                continue

            if event == 'start':
                