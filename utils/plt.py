import matplotlib.pyplot as plt
import numpy as np

labels = range(1, 23)  # x轴标签
data1 = [2.91017300e+03, 1.11779410e+04, 6.20650000e+01, 5.82671700e+03,
 8.83261200e+03, 5.03315000e+02, 2.54560500e+03, 4.69946000e+02,
 5.67877000e+02, 1.77924810e+04, 1.25114760e+04, 5.28322200e+03,
 2.66012300e+03, 7.26194300e+03, 1.81450580e+04, 1.10337983e+05,
 1.54482200e+03, 3.70184080e+04, 3.50237700e+03, 5.98594000e+02,
 2.25758970e+04, 3.94098000e+02]
data2 = [4069.013, 13869.364, 719.986, 8150.8, 13449.988, 907.56,
   5680.644, 2324.18, 2328.577, 19321.572, 16399.438, 6475.747,
   12044.119, 8880.624, 22534.053, 109018.462, 2220.477, 39333.924,
   5518.028, 768.963, 31237.108, 1696.74 ]

# 设置柱状图的宽度
bar_width = 0.25

# 设置x轴的位置
x = np.arange(len(labels))

# 创建柱状图
plt.bar(x - 0.5 * bar_width, data1, width=bar_width, label='Static', color='#1f77b4', align='center')  # 蓝色
plt.bar(x + 0.5 * bar_width, data2, width=bar_width, label='Dynamic', color='#ff7f0e', align='center')  # 橙色

# 添加标签和标题
plt.ylabel('Latency (ms)')
plt.title('AlloyDB Latency on Queries')
plt.xticks(x, labels)  # 设置x轴的标签
plt.legend()  # 显示图例

# 显示图形
plt.tight_layout()
plt.savefig('AlloyDB latency.png')
