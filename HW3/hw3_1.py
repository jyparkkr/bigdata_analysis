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

beta = 0.9
n = 1000

lines = sc.textFile(loc, 1)
line1 = lines.map(lambda l: re.split('\t',l))
line2 = line1.map(lambda x: (int(x[0]), int(x[1]))).distinct()
graph = line2.collect()

M = np.zeros((n, n), dtype = float)

for i in graph:
    M[i[1]-1, i[0]-1] = 1

num_linked = list()
for vec in M.transpose():
    c = 0
    for i in vec:
        if i==1:
            c+=1
    num_linked.append(c)

for i in range(n):
    M[:,i] /= num_linked[i]


# Method #1, Using Pyspark
Mat = sc.parallelize(M)
count = 0
_v = np.ones((n, 1), dtype = float) / n
while(count < 50):
    vec = Mat.flatMap(lambda m: np.matmul(m, _v))
    vec_list = vec.collect()
    _v = np.asarray(vec_list)
    _v.shape = (1000, 1)
    count += 1

sc.stop()


# # Method #2, Not using Pyspark
v = np.ones((n, 1), dtype = float) / n
e = np.ones((n, 1), dtype = float)
count = 0
while(count < 50):
    nextv = beta * np.matmul(M, v) + (1 - beta) * e / n
    v = nextv
    count +=1


# # Postprocessing
result1 = list()
for i in range(n):
    result1.append((i+1, v[i][0]))

sorted_result1 = sorted(result1, key = lambda x: x[1], reverse=True)

result2 = list()
for i in range(n):
    result2.append((i+1, _v[i][0]))

sorted_result2 = sorted(result2, key = lambda x: x[1], reverse=True)

#for i in range(10):
#    print(sorted_result1[i][0], '\t', sorted_result1[i][1])

for i in range(10):
    print(sorted_result2[i][0], '\t', sorted_result2[i][1])

# both results from sorted_result1 and sorted_result2 are almost same
