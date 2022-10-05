import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

naive = [
    422.21,
    244.54,
    344.36,
    165.23,
    155.80
]

cloud_optimized = [
    324.78,
    203.58,
    200.51,
    124.94,
    113.43
]

labels = [
    'Ray EC2\n(autoscaler)',
    'Ray EC2\n(fixed cluster)',
    'Lithops EC2\n(autoscaler)',
    'Lithops EC2\n(fixed cluster)',
    'Lithops\nAWS Lambda'
]

# sns.set_style('white')
sns.despine()
# sns.set_theme()

X = np.arange(len(labels))
fig, ax = plt.subplots()

ax.grid(axis='y', zorder=0, alpha=0.5, ls='--')

ax.bar(X - 0.175, naive, width=0.3, label='Static\npartitioning', zorder=3)
ax.bar(X + 0.175, cloud_optimized, width=0.3, label='Cloud-Optimized\npartitioning', zorder=3)

ax.set_xticks(X)
ax.set_xticklabels(labels)

ax.legend()

fig.tight_layout()
fig.savefig('bar_compare_all.png', dpi=300)
