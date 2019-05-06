
import numpy as np
import functools

import sys
import re
import os

from pyspark import SparkConf, SparkContext

#X_input = "./features.txt"
X_input = sys.argv[1]

#Y_input = "./labels.txt"
Y_input = sys.argv[2]


conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')

X_datasets = open(X_input, "r")
X_initial = np.genfromtxt(X_datasets, delimiter=',')

add_one = np.ones(6000).reshape(6000, 1)

labels = open(Y_input, "r")
y = np.genfromtxt(labels, delimiter=',').reshape(6000, 1)

X_datasets.close()
labels.close()

X = np.hstack((X_initial, add_one))
Datasets = np.hstack((y, X)) ##Matrix 'Datasets' is containing all y, X, b

eta = 10**-5
C = 1

mu, sigma = 0, 2
W = list()

accuracy = list()

def training(data, weight):
    grad = abs(weight)
    rdd = data    .map(lambda x: (x[0], x[1:]))    .map(lambda x: (x[0], x[1], x[0] * np.dot(weight, x[1])))    .filter(lambda x: x[2]<1).map(lambda x: (x[0], x[1]))    .map(lambda x: x[0]*x[1])
    
    L = rdd.reduce(lambda u, v: u + v)
    
    grad -= C*L[0]
    weight -= eta * grad

def testing(data, weight):
    rdd = data    .map(lambda x: (x[0], x[1:]))    .map(lambda x: x[0] * np.dot(weight,x[1]))    .filter(lambda x: x > 0)
    
    c = rdd.count()
    total = data.count()
    return c/total
    
for i in range(10):
    test = Datasets[600*i:600*(i+1),:]
    
    train1 = Datasets[:600*i,:]
    train2 = Datasets[600*(i+1):,:]
    train = np.vstack((train1,train2))
    
    """i th weight matrix would be W[i]"""
    W.append(np.random.normal(mu, sigma, 123))
    
    c = 0
    max_acc = 0
    it = 0
    while(c<10000):
        TrainSet = sc.parallelize(train)
        training(TrainSet, W[i])
        if(c%50 == 0):
            TestSet = sc.parallelize(test)
            acc = testing(TestSet,W[i])
            #print("Accuracy: %1.3f" %acc)
            if(acc > max_acc):
                max_acc = acc
                it = 0
            it += 1
            if(it > 15):
                break
            ##print(it)
        c+=1
    accuracy.append(acc)

final_acc = functools.reduce(lambda x, y: x+y, accuracy) / len(accuracy)
print("<%1.5f>" %final_acc)
print("<%1.5f>" %C)
print("<%1.5f>" %eta)

sc.stop()
