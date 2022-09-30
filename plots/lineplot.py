import json
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pprint import pprint

def do_plot():
    with open(f'results/pods.json') as data:
        pods = json.loads(data.read())
    
    with open(f'results/req_times_b1.json') as data:
        reqs_tups_b1 = json.loads(data.read())
    with open(f'results/req_times_b2.json') as data:
        reqs_tups_b2 = json.loads(data.read())

    # reqs_tups_b1, reqs_tups_b2 = requests
    # print(len(reqs_tups_b2))
    
    # pprint(pods)

    times_start = []
    times_end = []
    for pod in pods.values():
        if 'Running' in pod['status']:
            times_start.append(pod['status']['Running'])
        if 'Terminating' in pod['status']:
            times_end.append(pod['status']['Terminating']+45)
    times_start.sort()
    times_end.sort(reverse=True)

    t0 = times_start[0]
    t1 = times_end[0] - 35

    print(t0, t1)

    times = np.arange(int(t0), int(t1), 1)
    times_X = [int(t1)-t for t in times][::-1]
    print(times_X)

    pods_b1 = []
    for t in times:
        n_pods = 0
        for key, pod in pods.items():
            if 'bucket-1' in key:
                pt0 = pod['status'].get('Running', t0)
                pt1 = pod['status'].get('Terminating', t1)

                if t >= pt0 and t <= pt1:
                    n_pods += 1
        pods_b1.append(n_pods)
    
    pods_b2 = []
    for t in times:
        n_pods = 0
        for key, pod in pods.items():
            if 'bucket-2' in key:
                pt0 = pod['status'].get('Running', t0)
                pt1 = pod['status'].get('Terminating', t1)

                if t >= pt0 and t <= pt1:
                    n_pods += 1
        pods_b2.append(n_pods)
    
    print(pods_b1)
    print(pods_b2)

    reqs_b1 = []
    for t in times:
        reqs_n = 0
        for tup in reqs_tups_b1:
            if tup[0] <= t and tup[1] >= t:
                reqs_n += 1
        reqs_b1.append(reqs_n)

    reqs_b2 = []
    for t in times:
        reqs_n = 0
        for tup in reqs_tups_b2:
            if tup[0] <= t and tup[1] >= t:
                reqs_n += 1
        reqs_b2.append(reqs_n)
    
    print(reqs_b1)
    print(reqs_b2)

    fig, ax1 = plt.subplots()

    # ax.plot(times_X, pods_b1)
    # ax.plot(times_X, pods_b2)

    ax1.set_xlabel('Wallclock time (s)')

    ax1.plot(times_X, pods_b1, lw=1.5, ls='--', label='Running Pods\nfor bucket-1')
    ax1.fill_between(times_X, np.min(pods_b1), pods_b1, alpha=0.25)
    ax1.plot(times_X, pods_b2, lw=1.5, ls='--', label='Running Pods\nfor bucket-2')
    ax1.fill_between(times_X, np.min(pods_b2), pods_b2, alpha=0.25)
    ax1.set_yticks(np.arange(1, 10, 3))
    ax1.set_ylabel('No. Pods')

    ax2 = ax1.twinx()
    ax2.plot(times_X, reqs_b1, lw=2.5, label='Requests-in-flight\nfor bucket-1')
    ax2.plot(times_X, reqs_b2, lw=2.5, label='Requests-in-flight\nfor bucket-2')
    ax2.set_yticks(np.arange(1, 100, 10))
    ax2.set_ylabel('No. Requests in flight')

    # colors = ['pink', 'lightblue', 'lightgreen']
    # for bplot in (bp1, bp2):
    #     for patch, color in zip(bplot['boxes'], colors):
    #         patch.set_facecolor(color)

    # fig.set_size_inches(10, 8)
    # fig.suptitle('No-op PUT/GET 100 MiB payload')
    fig.tight_layout()
    handles, labels = handles, labels = [(a + b) for a, b in zip(ax1.get_legend_handles_labels(), ax2.get_legend_handles_labels())]
    plt.legend(handles, labels, loc='upper left',)
    # # plt.show()
    #fig.savefig(f'plot.pdf')
    fig.savefig(f'plot.png', dpi=300)




if __name__ == '__main__':
    sns.set_theme()
    do_plot()

# ax = sns.boxplot(x="x", y="elapsed", data=)