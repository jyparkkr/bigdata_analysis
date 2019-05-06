# This is assignment from KAIST EE412 Foundation of Big Data Analytics, Fall 2018 
## HW1
### (hw1_1.py) MapReduce and Spark - Find potential friends in a social network using Spark
    =============
    From graph of friend in social network, discover potential friends who have many mutual friends. We are interested in finding pairs of users who are 1) not friends with each other, but 2) have many common friends.
    For example, if A is a friend of B, B is a friend of C, but A is not a friend of C, then we are interested in the pair (A, C), which has one common friend B.
### (hw1_2.py) Frequent Itemsets - Find frequent itemsets using the A-Priori algorithm
    =============
    Implement A-Priori algorithm to find frequent items and item pairs in an online browsing dateset. 
    To store counts of pairs, use the triangular method with threshold of 200. 
    The output should contain number of the top-10 most frequent pairs. 
    For ties, sort by first id and then second id in alphabetical order.
### (hw1_3.py) Finding Similar Items - Find similar documents using minhash-based LSH
    =============
    In very similar articles within a large article set, use minhashed-based LSH algorithm to efficiently find articles that have high Jaccard similarities. Extract 3-shingles.

## HW2
### (hw2_1.py)Clustering -  Implement the k-Means algorithm using Spark
    =============
    Find right k value for the given data. For distance measure, use Euclidean distance. Implement the initialization of clusters for k-Means using a variant of the first approach in Chapter 7.3.2 using a sequential algorithm in **plain Python and not Spark**. Instead of picking the first point at random, however, use the first point in the dataset, which has already been randomized. 
    Then implement k-Means algorithm by using **Spark**. After run, your code should **print the average diameter** of the given number-of-cluster (k_value in the command-line).
### (hw2_3b.py)Recommendation Systems - Implement collaborative filtering
    =============
    Implement user-based and item-based collaborative filtering and run it on a real movie dataset.
    The rating file consists of about 90,000 lines of user ID, movie ID, rating(by 1-5) stars, timestamp.
    Implement the collaborative filtering algorithm where we use the cosine distance for measuring similarity. 
    1. User-based: Find the 10 most similar users and compute the average ratings for movie, only for the similar users who have rated.
    2. Item-based: Find the 10 most similar movies(by cosine distance) and take the average of the ratings that users gave to those similar movies.    
### (hw2_3c.py) Recommendation Systems - Movie Recommendation Challenge
    =============
    Similar to the NetFlix Challenge, we will have a EE412 Movie Recommendation Challenge. Improve the Recommendation System you implemented in 3(b) using any method you like (e.g., better normalization, clustering, different similarity measures, UV-decomposition, or even winning techniques used in the NetFlix Challenge)

## HW3


## HW4
  
