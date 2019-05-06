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
#loc = "./browsing.txt"
loc = sys.argv[1]

lines = sc.textFile(loc, 1)
threshold = 200


# In[3]:


## all modified lines are appended to list for easy debugging
## elem is for frequent_set; check whether each element is more than threshold
elem=list()
elem.append(lines.map(lambda l: re.split('\s',l))            .map(lambda l: set(l[:-1])))
elem.append(elem[-1].flatMap(lambda x: [(item, 1) for item in x]))
elem.append(elem[-1].reduceByKey(lambda x, y: x+y))
elem.append(elem[-1].filter(lambda x: x[1]>=threshold))
elem.append(elem[-1].map(lambda x: x[0]))


# In[4]:


freq_set = set(elem[-1].collect())


# In[5]:


## line is for getting pair which are both frequent
line=list()
line.append(elem[0].map(lambda x: x.intersection(freq_set)))


# In[6]:


## freq_dict and inv_dict is for changing items to index(which is integer)
## or vice versa
freq_list = list(freq_set)
freq_list.sort()
freq_dict = dict()
inv_dict = dict()
i = 0
for item in freq_list:
    freq_dict[item]=i
    inv_dict[i]=item
    i+=1


# In[7]:


def change_to_index(x):
    index_set = set()
    for item in x:
        index_set.add(freq_dict[item])
    return index_set


# In[8]:


line.append(line[-1].map(change_to_index))
freqline=line[-1].collect()


# In[9]:


f = len(freq_set)
tri_mat = list()
for i in range(f):
    for j in range(i+1, f):
        #print(i, j)
        count = 0
        for bucket in freqline:
            if {i, j}.issubset(bucket):
                count+=1
                #print("count: ",count)
        tri_mat.append(count)
        #print(i, j)


# In[10]:


from math import sqrt
def get_idx(c, frq_len):
    i=0
    line = frq_len - 1
    while((c-line)>=0):
        c-=line
        line-=1
        i+=1
    j=i+c+1
    return (-i, -j)
    ## returning negative value for reverse sorting
    ## value: descending, id: ascending


# In[11]:


idx=0
frq_list=list()
for count in tri_mat:
    if count>threshold:
        frq_list.append((get_idx(idx, len(freq_dict)), count))
    idx+=1

neg_sorted_list = sorted(frq_list, key=lambda tup: (tup[1], tup[0][0]), reverse=1)


# In[12]:


sorted_list = list(map(lambda x: ((-x[0][0], -x[0][1]), x[1]), neg_sorted_list))
translated_list = list(map(lambda x: ((inv_dict[x[0][0]], inv_dict[x[0][1]]), x[1]), sorted_list))


# In[13]:


def save(filename, contents):
    fh = open(filename, 'w')
    fh.write(contents)
    fh.close()


# In[14]:


st=str()

print("number of frequent items: ",len(freq_dict))
st+="number of frequent items: "
st+=str(len(freq_dict))
st+='\n'
print("number of frequent pairs: ",len(translated_list))
st+="number of frequent pairs: "
st+=str(len(translated_list))
st+='\n'

c=0
for t in translated_list:
    st+=t[0][0]
    st+='\t'
    st+=t[0][1]
    st+='\t'
    st+=str(t[1])
    st+='\n'
    if(c<10):
        print(str(t[0][0]),'\t',str(t[0][1]),'\t',str(t[1]))
    c+=1
    if(c>=10):
        break


# In[15]:

# time needed for all this process: 5min 20s
save('2-b.txt', st)
sc.stop()


# In[ ]:




