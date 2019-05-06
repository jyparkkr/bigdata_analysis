import sys
import os
import numpy as np
import math

#loc1 = "ratings.txt"
#loc2 = "ratings_test.txt"
loc1 = sys.argv[1]
loc2 = sys.argv[2]


train = open(loc1, "r")
train_data = np.genfromtxt(train, delimiter=',')
train.close()
test = open(loc2, "r")
test_data = np.genfromtxt(test, delimiter=',')
test.close()


users_train = list()
for i in train_data:
    if i[0] not in users_train:
        users_train.append(int(i[0]))
users_train.sort()

movies_train = list()
for i in train_data:
    if i[1] not in movies_train:
        movies_train.append(int(i[1]))
movies_train.sort()

users_test = list()
for i in train_data:
    if i[0] not in users_test:
        users_test.append(int(i[0]))
users_test.sort()

movies_test = list()
for i in test_data:
    if i[1] not in movies_train:
        movies_test.append(int(i[1]))
movies_test.sort()

users = list(set(users_train).union(set(users_test)))
movies = list(set(movies_train).union(set(movies_test)))

rating = np.zeros((len(users), len(movies)))
output_list = list() # to append timestamp

for i in train_data:
    user_idx = users.index(int(i[0]))
    movie_idx = movies.index(int(i[1]))
    rating[user_idx][movie_idx] = i[2]
    
for i in test_data:
    user_idx = users.index(int(i[0]))
    movie_idx = movies.index(int(i[1]))
    rating[user_idx][movie_idx] = i[2]
    
    output_list.append(((int(i[0]), int(i[1])), i[3]))

normed_rating = np.zeros((len(users), len(movies)))

for i in range(len(users)):
    sq = np.sum(rating[i][np.invert(np.isnan(rating[i]))] ** 2)
    s = np.sum(rating[i][np.invert(np.isnan(rating[i]))])
    c = np.count_nonzero(rating[i][np.invert(np.isnan(rating[i]))])
    avg = s / c
    sq_avg = sq / c
    std = math.sqrt(sq_avg - math.pow(avg, 2))
    for j in range(len(movies)):
        elem = rating[i][j]
        if (elem != 0):
            normed_rating[i][j] =  (elem - avg) / std
        if (np.isnan(elem)):
            normed_rating[i][j] =  np.nan

def cos_distance(x, y):
    x_nan = np.invert(np.isnan(x))
    y_nan = np.invert(np.isnan(y))
    not_nan = x_nan * y_nan
    x = x[not_nan]
    y = y[not_nan]
    num = np.dot(x, y)
    denom = math.sqrt(np.dot(x, x) * np.dot(y, y))
    if(denom!=0):
        cos_sim = num / denom
    else:
        cos_sim = -1
    try:
        math.acos(cos_sim) # return as radian
    except ValueError:
        cos_sim = int(cos_sim) # while value is, for instance, 1.000001
    return math.acos(cos_sim) # return as radian

def similar_users(i, n): # i is index of target user, n is number of similar user wanted
    dist_list = list()
    normed_target = normed_rating[i]
    for normed_user in normed_rating:
        dist = cos_distance(normed_target, normed_user)
        dist_list.append(dist)
    sorted_dist_list = sorted(dist_list)
    sim_users = list()
    for i in range(1, n+1):
        user_idx = dist_list.index(sorted_dist_list[i])
        sim_users.append(rating[user_idx])
    nparray = np.asarray(sim_users)
    return nparray # numpy array of similar users in ascending order

rating_predicted = list()
for i in range(len(users)):
    similar_users_list = similar_users(i, 7)
    likes = np.transpose(similar_users_list)
    for j in range(len(movies)):
        likes_movie = likes[j]
        elem = rating[i][j]
        if (np.isnan(elem)):
            s = np.sum(likes_movie[np.invert(np.isnan(likes_movie))])
            c = np.count_nonzero(likes_movie[np.invert(np.isnan(likes_movie))])
            user_idx = users[i]
            movie_idx = movies[j]
            if(c!=0):
                predict = s/c
            else: # result is average of rating given by user
                ss = np.sum(rating[i][np.invert(np.isnan(rating[i]))])
                cc = np.count_nonzero(rating[i][np.invert(np.isnan(rating[i]))])
                if cc!=0:
                    predict = ss / cc
                else:
                    predict = 2.5
            rating_predicted.append(((user_idx, movie_idx), predict))

def save(filename, contents):
    fh = open(filename, 'w')
    fh.write(contents)
    fh.close()

st = str()
for i in output_list:
    match = list(filter(lambda j: i[0]==j[0], rating_predicted))
    user = i[0][0]
    movie = i[0][1]
    rating = match[0][1]
    timestamp = int(i[1])
    st+=str(user)
    st+=str(',')
    st+=str(movie)
    st+=str(',')
    st+=str(rating)
    st+=str(',')
    st+=str(timestamp)
    st+=str('\n')

save('output.txt', st)

