import sys
import os
import numpy as np
import math

#loc = "ratings.txt"
loc = sys.argv[1]

doc = open(loc, "r")
initial_data = np.genfromtxt(doc, delimiter=',')[:,0:3] # remove <TIMESTAMP>
doc.close()

users = list()
for i in initial_data:
    if i[0] not in users:
        users.append(int(i[0]))
users.sort()

movies = list()
for i in initial_data:
    if i[1] not in movies:
        movies.append(int(i[1]))
movies.sort()

user_based = np.zeros((len(users), len(movies)))


# numpy matrix "user_based" and "movie_based" have rating of each movie for each user.
# Each row and column of "user_based" are (sorted) user and movie, and element of matrix is ratings.
# Each row and column of "movie_based" are (sorted) movie and user, and element of matrix is ratings.
# 

for i in initial_data:
    user_idx = users.index(int(i[0]))
    movie_idx = movies.index(int(i[1]))
    user_based[user_idx][movie_idx] = i[2]

normed_user_based = np.zeros((len(users), len(movies)))

avg_list = list()
for i in range(len(users)):
    s = np.sum(user_based[i]) #sum of ratings
    c = np.count_nonzero(user_based[i])
    avg = s / c
    avg_list.append(avg)
    for j in range(len(movies)):
        elem = user_based[i][j]
        if (elem != 0):
            normed_user_based[i][j] =  elem - avg

movie_based = user_based.transpose()
normed_movie_based = normed_user_based.transpose()

def cos_distance(x, y):
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

def similar_users(i): # i is index of target user
    dist_list = list()
    normed_target = normed_user_based[i]
    for normed_user in normed_user_based:
        dist = cos_distance(normed_target, normed_user)
        dist_list.append(dist)
    sorted_dist_list = sorted(dist_list)
    sim_users = list()
    for i in range(1, 11):
        user_idx = dist_list.index(sorted_dist_list[i])
        sim_users.append(user_based[user_idx])
    nparray = np.asarray(sim_users)
    return nparray # numpy array of similar users in ascending order

user_predicted = user_based.copy()
for i in range(len(users)):
    similar_users_list = similar_users(i)
    movie_based_similar_users = np.transpose(similar_users_list)
    for j in range(len(movies)):
        elem = user_predicted[i][j]
        if (elem == 0):
            s = np.sum(movie_based_similar_users[j]) #sum of ratings
            c = np.count_nonzero(movie_based_similar_users[j])
            if(c!=0):
                user_predicted[i][j] = s/c
        else:
            user_predicted[i][j] = -1 # change original value to -1, remains only predicted value

user_id = users.index(600)
user_target_rating = (user_predicted[user_id]).tolist()
user_original_rating = (user_based[user_id]).tolist()
user_target_rating_sort = sorted(user_target_rating, reverse = True)

print("User-based method")
i=0
c=0
while(c<5):
    j = user_target_rating.index(user_target_rating_sort[i])
    movie_id = movies[j]
    if(j==len(movies)):
        break
    if(movie_id<=1000):
        print(movie_id, '\t', user_target_rating[j])
        c+=1
    user_target_rating[j]=0
    i+=1
## additional movies, e.g. 175, 261, 440, 480, 527, 832, 899 have their rating 5.0

def similar_movies(i): # i is index of target movie
    dist_list = list()
    normed_target = normed_movie_based[i]
    for normed_movie in normed_movie_based:
        dist = cos_distance(normed_target, normed_movie)
        dist_list.append(dist)
    sorted_dist_list = sorted(dist_list)
    sim_movies = list()
    for i in range(1, 11):
        movie_idx = dist_list.index(sorted_dist_list[i])
        sim_movies.append(movie_based[movie_idx])
    nparray = np.asarray(sim_movies)
    return nparray # numpy array of similar users in ascending order

movie_predicted = movie_based.copy()
for i in range(len(movies)):
    similar_movies_list = similar_movies(i)
    user_based_similar_movies = np.transpose(similar_movies_list)
    for j in range(len(users)):
        elem = movie_predicted[i][j]
        if (elem == 0):
            s = np.sum(user_based_similar_movies[j]) #sum of ratings
            c = np.count_nonzero(user_based_similar_movies[j])
            if (c != 0):
                movie_predicted[i][j] = s/c
        else:
            movie_predicted[i][j] = -1 # change original value to -1, remains only predicted value
            
movie_predicted.transpose()

user_id = users.index(600)
movie_target_rating = (movie_predicted[user_id]).tolist()
movie_original_rating = (user_based[user_id]).tolist()
movie_target_rating_sort = sorted(movie_target_rating, reverse = True)
print('\n')
print("Movie-based method")
i=0
c=0
while(c<5):
    j = movie_target_rating.index(movie_target_rating_sort[i])
    movie_id = movies[j]
    if(j==len(movies)):
        break
    if(movie_id<=1000):
        print(movie_id, '\t', movie_target_rating[j])
        c+=1
    movie_target_rating[j]=0
    i+=1
# also additional movies have their rating 5.0.

