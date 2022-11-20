# Spark (35 pts): 

In this question, you will improve your Spark programming skills and practice composing an algorithm into map and reduce functions. Implementing a simple friendship recommendation algorithm using Spark will also help you with the next assignments and the class project. 

Write a Spark program that implements a simple ``People You Might Know'' social network friendship recommendation algorithm. The key idea is that if two people have a lot of mutual friends, then the system should recommend that they connect with each other.

**Data**

* Associated data file is `soc-LiveJournal1Adj.txt` in q1/data.
* The file contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>

Here, <User> is a unique integer ID corresponding to a unique user, and 
<Friends> is a comma separated list of unique IDs corresponding to the friends of the user with the unique ID <User>. 
Note that the friendships are mutual (i.e., edges are undirected): if A is friend with B then B is also friend with A. The data provided is consistent with that rule as there is an explicit entry for each side of each edge.

**Algorithm**

Let us use a simple algorithm such that, for each user U, the algorithm recommends N = 10 users who are not already friends with U, but have the most number of
mutual friends in common with U.

**Output**

* The output should contain one line per user in the following format:
<User><TAB><Recommendations>, where <User> is a unique ID corresponding to a user and <Recommendations> is a comma separated list of unique IDs corresponding to the algorithm’s recommendation of people that <User> might know, ordered in decreasing number of mutual friends.

* Even if a user has less than 10 second-degree friends, output all of them in decreasing order of the number of mutual friends. If a user has no friends, you can provide an empty list of recommendations. If there are recommended users with the same number of mutual friends, then output those user IDs in numerically ascending order.

**Pipeline sketch**

Please provide a description of how you used Spark to solve this problem. Don’t write more than 3 to 4 sentences for this: we only want a very high-level description of your strategy to tackle this problem.


**Tips**

* Before submitting a complete application to Spark, you may use the Shell to go line by line, checking the outputs of each step. Command .take(X) should be helpful, if you want to check the first X elements in the RDD.
* For sanity check, your top 10 recommendations for user ID 11 should be: 27552, 7785, 27573, 27574, 27589, 27590, 27600, 27617, 27620, 27667.
* The default memory assigned to the Spark runtime may not be enough to process this data file, depending on how you write your algorithm. If your Spark job fails with a message starting as: 

```
17/12/28 10:50:35 INFO DAGScheduler: Job 0 failed: sortByKey at FriendsRecomScala.scala:45, took 519.084974 s
Exception in thread "main" org.apache.spark.SparkException:
Job aborted due to stage failure: Task 0 in stage 2.0 failed 1 times, most recent failure:
Lost task 0.0 in stage 2.0 (TID 4, localhost, executor driver)
```

then you’ll very likely need to increase the memory assigned to the Spark runtime. If you are running in stand-alone mode (i.e. you did not setup a Spark cluster), use
--driver-memory 8G to set the runtime memory to 8GB. If you are running on a Spark cluster, use --executor-memory 8G to set the memory to 8GB.

## What to submit? 
1. Upload your code to Gradescope.
2. Include in your writeup a short paragraph sketching your spark pipeline. You can write it here or create a new markdown file.
3. Include in your writeup the recommendations for the users with following user IDs: 924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993