import math

import sys
import os

#loc = "./sample.txt"
loc = sys.argv[1]

k_list = sys.argv[2:]

stream = open(loc, "r")

c = 0
j = 0
window = list()
while True:
    line = stream.readline()
    if not line:
        break
    label = int(line)
    #print(label)
    if label:
        if(len(window)==0):
            window.append([c])
            c+=1
            continue
        i = 0
        window[i].insert(0, c)
        while(len(window[i]) == 3):
            window[i].pop()
            temp = window[i].pop()
            i += 1
            if(i == len(window)):
                window.append([temp])
                
            else:
                window[i].insert(0, temp)

    c+=1
    
stream.close()

def nextp(p, i, win = window):
    if(p == win[i][0]):
        if(len(win[i])==2):
            return (win[i][1], i)
    
    i+=1
    if(len(win)==i):
        return (-1, i)
    
    return (win[i][0], i)
    
for k in k_list:
    k_int = int(k)
    find_until = c - k_int

    i = 0
    s = 0 ## estimated number of 1 in last k bits
    point = window[i][0]
    while(point >= find_until):
        if nextp(point, i)[0] < find_until:
            s += math.ceil(2**(i-1))
        else:
            s += math.ceil(2**i)
        (point, i) = nextp(point, i)
    
    #print(k,s,i)
    print(s)

