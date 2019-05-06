#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import re
import os
from pyspark import SparkConf, SparkContext


# In[2]:


conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
#loc = "./soc-LiveJournal1Adj.txt"
loc = sys.argv[1]

lines = sc.textFile(loc, 1)


# In[3]:


## all modified lines are appended to list for easy debugging
line=list()
line.append(lines.map(lambda l: re.split('\t',l)))
line.append(line[-1].mapValues(lambda l: re.split(',', l)))
line.append(line[-1].filter(lambda x: x[1][0]!=''))
line.append(line[-1].flatMap(lambda x: [(x[0],item) for item in x[1]]))
line.append(line[-1].map(lambda x: (int(x[0]), int(x[1]))))
line.append(line[-1].map(lambda x: (min(x[0], x[1]), max(x[0], x[1]))))
line.append(line[-1].distinct().sortByKey())
line.append(line[-1].groupByKey().mapValues(lambda x: set(x)))


# In[4]:


dic = line[-1].collectAsMap()


# In[14]:


## function get_friend returns key-value pair of 
## target and friend's friend list
def get_friend(pair):
    target = pair[0]
    friend = pair[1]
    #friend: set, dic: friend dictionary
    #global dic
    #dic = {1: {2, 3, 4}, 2: {3, 5}, 3: {}}
    friendnearby = set()
    for f in friend:
        try:
            ff = dic[f]
            friendnearby=friendnearby.union(ff)
        except KeyError:
            pass
    friendnearby=friendnearby.difference(friend)
    friendnearbylist = list(friendnearby)
    friendnearbylist.sort()
    
    return (target, friendnearbylist)


# In[6]:


friend=list()
friend.append(line[-1].map(get_friend))
friend.append(friend[-1].flatMap(lambda x: [(x[0],item)                                             for item in x[1]]))


# In[7]:


## this function returns possible pairs
def get_mutual(pair):
    f1 = pair[0]
    f2 = pair[1]
    try:
        intersect = dic[f1]&dic[f2]
        count = len(intersect)
    except KeyError:
        count = 0
    return (count, (f1, f2))
    


# In[8]:


friend.append(friend[-1].map(get_mutual))
friend.append(friend[-1].groupByKey()              .mapValues(lambda x: list(x)).sortByKey(ascending=0))


# In[9]:


def sort_ppl(x):
    count = x[0]
    ppl_list = x[1]
    sorted_list = sorted(ppl_list, key=lambda tup: tup[0])
    
    return (count, sorted_list)


# In[10]:


friend.append(friend[-1].map(sort_ppl))
friend.append(friend[-1].flatMap(lambda x: [(x[0],item)                                            for item in x[1]]))
friend.append(friend[-1].map(lambda x: (x[1][0], x[1][1], x[0])))
friend.append(friend[-1].filter(lambda x: x[2]>0))


# In[11]:


#output is saved as txt to check output easily
def save(filename, contents):
    fh = open(filename, 'w')
    fh.write(contents)
    fh.close()


# In[12]:

count=0
st=''
result = friend[-1].collect()
for t in result:
    st+=str(t[0])
    st+='\t'
    st+=str(t[1])
    st+='\t'
    st+=str(t[2])
    st+='\n'
    if(count<10):
        print(str(t[0]),'\t',str(t[1]),'\t',str(t[2]))
    count+=1
save('hw1-1.txt', st)


# In[13]:

# time needed for all this process: 12s
sc.stop()

