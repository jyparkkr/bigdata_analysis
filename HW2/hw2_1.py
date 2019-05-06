import sys
import re
import os
import numpy as np
import math
from pyspark import SparkConf, SparkContext

loc = sys.argv[1]
in_k = sys.argv[2]
k = int(in_k)
km = open(loc, 'r')
#k =  ## number wanted for k-mean

initial_data = np.loadtxt(km)

def get_distance(a, b):
    if len(a)!=len(b):
        return 0
    l = len(a)
    d = 0
    for i in range(l):
        d = d + (a[i] - b[i])**2
    return math.sqrt(d)

def find_far_point(datasets, points):
    check = list()
    for d in datasets:
        dist = np.inf
        for p in points:
            dist = min(dist, get_distance(d, p))
        check.append(dist)
    #print(max(check))
    return check.index(max(check))

def modify_centroids(datasets, points, idx):
    selected = np.array(datasets[idx], dtype=np.float64)
    selected.shape = 1, 58
    new_points = np.vstack([points, selected])
    return new_points  

# picked point initialization
initial_point = np.array(initial_data[0], dtype=np.float64)
initial_point.shape = 1, 58
#print(initial_point)

data = initial_data
centroids = initial_point

for i in range(k-1):
    idx = find_far_point(data, centroids)
    centroids = modify_centroids(data, centroids, idx)
    
centroid_list = centroids.tolist()

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')

points = sc.parallelize(initial_data)

p1 = points.map(lambda x: (x, -1)) # (point, index to centroid) # index -1 means not assigned yet

# assign initial points which are suppose to be centroid
def initial_assignment(x):
    point = x[0].tolist()
    idx = x[1]
    if point in centroid_list:
        idx = centroid_list.index(point)
    return (x[0], idx)

# assign to nearest point
def assign_point(x):
    point = x[0]
    idx = x[1]
    if(idx!=-1):
        return (point, idx)
    check = list()
    for c in centroids:
        dist = get_distance(c, point)
        check.append(dist)
    idx = check.index(min(check))
    return (point, idx)

p2 = p1.map(initial_assignment) # 
p3 = p2.map(assign_point)
p4 = p3.map(lambda x: (x[1], x[0]))
p5 = p4.groupByKey().mapValues(lambda x: list(x))

def get_diameter(points):
    diameter = 0
    l = len(points)
    for i in range(l):
        for j in range(i+1, l):
            diameter = max(diameter, get_distance(points[i],points[j]))
    return diameter

p6 = p5.map(lambda x: (x[0], get_diameter(x[1])))
p7 = p6.map(lambda x: (0, x[1]))
p8 = p7.reduceByKey(lambda u, v: u+v)

dist = p8.collect()
dist_sum = dist[0][1]
dist_avg = dist_sum/k

print(k, ": ",dist_avg)

sc.stop()

