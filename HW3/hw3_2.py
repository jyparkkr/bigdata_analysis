import numpy as np
from numpy import linalg as LA

import sys
import re
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')

#loc = "./graph.txt"
loc = sys.argv[1]

n = 1000

lines = sc.textFile(loc, 1)
line1 = lines.map(lambda l: re.split('\t',l))
line2 = line1.map(lambda x: (int(x[0]), int(x[1]))).distinct()
graph = line2.collect()

L = np.zeros((n, n), dtype = float)

for i in graph:
    L[i[0]-1, i[1]-1] = 1

Lt = L.transpose()

# # Using Spark

_L = sc.parallelize(L)
_Lt = sc.parallelize(Lt)

count = 0
_h = np.ones((n, 1), dtype = float)

while(count < 50):
    vec1 = _L.flatMap(lambda x: np.matmul(x, _h))
    a = vec1.collect()
    _a = np.asarray(a)
    _a = _a / np.max(_a)
    _a.shape = (1000, 1)
    vec2 = _Lt.flatMap(lambda x: np.matmul(x, _a))
    h = vec2.collect()
    _h = np.asarray(h)
    _h = _h / np.max(_h)
    _h.shape = (1000, 1)
    count += 1


# # Postprocessing

h_result = list()
a_result = list()
for i in range(n):
    h_result.append((i+1, _h[i][0]))
    a_result.append((i+1, _a[i][0]))

sorted_h_result = sorted(h_result, key = lambda x: x[1], reverse = True)
sorted_a_result = sorted(a_result, key = lambda x: x[1], reverse = True)

for i in range(10):
    print(sorted_h_result[i][0], '\t', sorted_h_result[i][1])

for i in range(10):
    print(sorted_a_result[i][0], '\t', sorted_a_result[i][1])

