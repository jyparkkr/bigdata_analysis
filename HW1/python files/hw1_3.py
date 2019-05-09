#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import re
import os
from pyspark import SparkConf, SparkContext

import random


# In[2]:


conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')

#loc = "./articles.txt"
loc = sys.argv[1]

lines = sc.textFile(loc, 1)


# In[3]:


## all modified lines are appended to list for easy debugging
line=list()
## this function is just for seperating document title from main article
line.append(lines.map(lambda l: re.split(r'[^\w]+',l)))
line.append(line[-1].map(lambda x: (x[0], x[1:])))


# In[4]:


## this function is for satisfying conditions from guideline
## filtering non-alphabetic word / change to lowercase
def get_wanted(x):
    doc = x[0]
    words = x[1]
    filtered = list()
    for w in words:
        if w.isalpha():
            filtered.append(w.lower())
    return (doc, filtered)


# In[5]:


line.append(line[-1].map(get_wanted))


# In[6]:


## considering a shingle unit as an three alphabetic character in row.
## not parsing the text by word
def get_3_shingles(x):
    shingles_set=set()
    article_length = len(x)
    for i in range(article_length-2):
        shingles_set.add(x[i]+x[i+1]+x[i+2])
    return shingles_set


# In[7]:


line.append(line[-1].mapValues(lambda x: ' '.join(x)))
line.append(line[-1].mapValues(get_3_shingles))


# In[8]:


shingles = line[-1].map(lambda x: x[1]).reduce(lambda v1, v2: v1|v2)


# In[9]:


## shingles_dict and inv_dict is for changing shingles to index
## (which is integer)
## or vice versa
shingles_dict=dict()
inv_dict=dict()
c=0
for s in shingles:
    shingles_dict[s]=c
    inv_dict[c]=s
    c+=1


# In[10]:


def shingles_to_hash(x):
    hashed=set()
    for s in x:
        hashed.add(shingles_dict[s])
    return hashed


# In[11]:


def hash_to_shingles(x):
    shingles=set()
    for h in x:
        shingles.add(inv_dict[h])
    return shingles


# In[12]:


## characteristic matrix
c_matrix = line[-1].mapValues(shingles_to_hash)


# number of shingles are 8181
# 
# n = 120 (wanted hash funcion row)
# 
# b = 6
# 
# r = 20
# (for threshold to be about 0.9)
# 
# 
# len(shingles_dict) = 8181
# 
# c = 8191 (smallest prime >= 8181)
# 
# a = random.randint(1, c) #cannot hash if a = 0
# 
# b = random.randint(0, c)

# In[13]:


def isPrime(n):
    if n < 2: return False
    for x in range(2, int(n**0.5) + 1):
        if n % x == 0:
            return False
    return True


# In[14]:


def generate_random_hash():
    n = len(shingles_dict)
    c = n
    while not isPrime(c):
        c+=1
    a = random.randint(1, c-1)
    b = random.randint(0, c-1)
    order=list()
    for i in range(n):
        h=(a*i+b)%c
        order.append(h)
    return order


# In[15]:


def get_hash_value(hash_list, x_set):
    c=0
    for x in hash_list:
        if x in x_set:
            return c
        c+=1


# In[16]:


def make_list(x):
    title=x[0]
    value=x[1]
    hashed=value[0]
    added=value[1]
    hashed.append(added)
    return (title, hashed)


# In[17]:


sig_mat=list()
for b in range(6):
    row = c_matrix.mapValues(lambda x: list()) #for initialization
    for r in range(20):
        h = generate_random_hash()
        row = row.join(c_matrix.mapValues(lambda x: get_hash_value(h, x)))
        row = row.map(make_list)
    sig_mat.append(row)


# In[18]:


## sig_list1,2,... are for modifying band/row
sig_list1=list()
for b in sig_mat:
    b1 = b.map(lambda x: (tuple(x[1]), x[0]))
    sig_list1.append(b1)


# In[19]:


sig_list2=list()
for b in sig_list1:
    b1 = b.groupByKey().mapValues(lambda x: set(x))
    sig_list2.append(b1)


# In[20]:


sig_list3=list()
for b in sig_list2:
    b1 = b.filter(lambda x: len(x[1])>1)
    sig_list3.append(b1)


# In[21]:


sig_list4=list()
for b in sig_list3:
    b1 = b.map(lambda x: x[1])
    sig_list4.append(b1)


# In[22]:


sig_list=0
for i in sig_list4:
    if sig_list==0:
        sig_list=i
    else:
        sig_list=sig_list.union(i)


# In[23]:


sig_list=sig_list.map(lambda x: list(x)).map(lambda x:(min(x[0], x[1]), max(x[0], x[1])))

sig_list=sig_list.distinct()


# In[24]:


sig_pair=sig_list.collect()


# In[25]:


## get number of same object from two lists
def get_same(x):
    if(len(x[0])!=len(x[1])):
        return 0
    length = len(x[0])
    c=0
    for i in range(length):
        if x[0][i]== x[1][i]:
            c+=1
    return c


# In[26]:


result=list()
for pair in sig_pair:
    a1=min(pair[0],pair[1])
    a2=max(pair[0],pair[1])
    a1_hash=list()
    a2_hash=list()
    sim = 0 ## similarity
    for b in sig_mat:
        b1 = b.filter(lambda x: x[0] in pair)
        b2 = b1.map(lambda x: x[1])
        b3 = b2.collect()
        sim+=get_same(b3)
    result.append((a1, a2, sim/120))


# In[27]:


sorted_result = sorted(result, key=lambda x: x[2], reverse=1)


# In[28]:


def save(filename, contents):
    fh = open(filename, 'w')
    fh.write(contents)
    fh.close()


# In[29]:


st=str()
for p in sorted_result:
    st+=str(p[0])
    st+='\t'
    st+=str(p[1])
    st+='\t'
    st+=str(p[2])
    st+='\n'
    print(f"{p[0]}\t{p[1]}\t{p[2]:.4f}")
    


# In[30]:

# time needed for all this process: 1min 30s
save('3-b.txt', st)
sc.stop()


# In[ ]:




