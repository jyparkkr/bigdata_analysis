## HW1
    HW1 is about basic usage and application of pyspark.
### (hw1_1.py) MapReduce and Spark
#### Find potential friends in a social network using Spark
    From graph of friend in social network, discover potential friends who have many mutual friends. 

    Each line is in the following format
    
    <User><TAB><Friends>
    
    Where <User> is an integer ID corresponding to user, <TAB> is tab character, and <Friends> is a comma-separated list of IDs of friends.

    We are interested in finding pairs of users who are 1) not friends with each other, but 2) have many common friends.
    For example, if A is a friend of B, B is a friend of C, but A is not a friend of C, 
    then we are interested in the pair (A, C), which has one common friend B.

    Find top-10 user paris with their counts.


### (hw1_2.py) Frequent Itemsets
#### Find frequent itemsets using the A-Priori algorithm
    Suppose you are an online retailer like Amazon and want to improve the shopping experience by analyzing customer behavior. 
    In this situation, implement A-Priori algorithm to find frequent items and item pairs in an online browsing dateset. 
    To store counts of pairs, use the triangular method with threshold of 200. 
    The output should contain number of the top-10 most frequent pairs. 
    For ties, sort by first id and then second id in alphabetical order.


### (hw1_3.py) Finding Similar Items
#### Find similar documents using minhash-based LSH
    Suppose you are looking for very similar articles within a large article set. 
    Use minhashed-based LSH algorithm to efficiently find articles that have high Jaccard similarities. 
    Ignore non-alphabet characters, convert all characters to lower case and extract 3-shingles.
    When generating random hash functions, use the hash function (ax + b)%c. Then set c to be the smallest prime number larger than or equal to n. Set a and b random integer between [0, c-1]
    Set b and r so that the threshold is about 0.9. You can set b = 6 and r = 20, unless you prefer a different setting.
    This problem does not require Spark programming, and your code does not have to be long.

