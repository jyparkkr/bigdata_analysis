import numpy as np
import functools

import sys
import os

#X_input = "./features.txt"
X_input = sys.argv[1]

#Y_input = "./labels.txt"
Y_input = sys.argv[2]


datasets = open(X_input, "r")
X_initial = np.genfromtxt(datasets, delimiter=',')

add_one = np.ones(6000).reshape(6000, 1)

labels = open(Y_input, "r")
y = np.genfromtxt(labels, delimiter=',')


datasets.close()
labels.close()

X = np.hstack((X_initial, add_one))

eta = 10**-5
C = 1

mu, sigma = 0, 2
W = list()

accuracy = list()

def training(idx, weight):
    grad = abs(weight)
    for i in idx:
        if(y[i] * np.dot(weight, X[i]) < 1):
            grad -= C * y[i] * X[i]
    weight -= eta * grad

def testing(idx, weight):
    c = 0
    for i in idx:
        if(y[i] * np.dot(weight, X[i]) > 0):
            c += 1
    return c/len(idx)
    
for i in range(10):
    test = list(range(600*i,600*(i+1)))
    train1 = list(range(0, 600*i))
    train2 = list(range(600*(i+1), 6000))
    train = train1 + train2
    """i th weight matrix would be W[i]"""
    W.append(np.random.normal(mu, sigma, 123))

    c = 0
    max_acc = 0
    it = 0
    while(c<10000):
        training(train, W[i])
        if(c%50 == 0):
            acc = testing(test,W[i])
            ##print("Accuracy: %1.3f" %acc)
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
