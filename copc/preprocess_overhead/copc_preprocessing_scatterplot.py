import json
import numpy as np

import matplotlib
import matplotlib.pyplot as plt

if __name__ == '__main__':
    with open('results.json', 'r') as results_file:
        results = json.loads(results_file.read())
    
    times = [r['preprocess_time'] for r in results]
    sizes = [r['size'] / 1048576 for r in results]
    sortes_tups = sorted(zip(sizes, times), key= lambda t: t[1])[3:-3]

    sizes_X = np.array([t[0] for t in sortes_tups])
    times_Y = np.array([t[1] for t in sortes_tups])

    fig, ax = plt.subplots(1, 1)
    ax.scatter(sizes_X, times_Y)
    ax.set_ylabel('Preprocessing time (s)')
    ax.set_xlabel('LAZ file size (MiB)')

    fig.tight_layout()
    fig.savefig('scatter.png', format='png')