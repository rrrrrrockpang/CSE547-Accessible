# CSE547 - Colab 8
# Submodular Optimization

# Setup
# we import some of the common libraries for our task.
import pandas as pd
import numpy as np
import time, logging

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/code.log", "w+"),
                            logging.StreamHandler()
                    ]
)

import warnings
warnings.filterwarnings("ignore")

from heapq import heappop, heappush

# Your Task
# If you run successfully the setup stage, you are ready to work with the Book-Crossing (BX) dataset. If you want to read more about it, check the [official website](http://www2.informatik.uni-freiburg.de/~cziegler/BX/) of the dataset, which contains concise schema description of the dataset, and the download page.
# In this Colab exersie, we will use the [Pandas API](https://pandas.pydata.org/docs/user_guide/index.html) to perform submodular optimiazation algorithms on a small subset of the BX-Book-Ratings dataset (20K users).
data = pd.read_csv('./data/book_ratings.csv')
data.head()

# Let's see the top 10 readers who have rated the most books, sorted in descending order.
data.groupby('userid').isbn.nunique().sort_values(ascending=False)[:10]

# Your goal is to find the set of users who rated the most unique books by implementing the greedy submodular optimization algorithm and the lazy greedy algorithm. Note: A user may give a rating of 0 to a book.

# Q1: Greedy Submodular Optimization Algorithm
# We start by determining our objective function. Write a function that returns the number of unique books rated by the set of users A.
logging.info('Q1: Greedy Submodular Optimization Algorithm')
def F(A):
  # YOUR CODE HERE 
  pass

# The greedy(k) function below provides a sketch for the greedy submodular optimization algorithm where the maximum cardinality is set to be k. Your task is to complete the function and report the set of users and the objective values (i.e. number of unique books) for k = 2 and 3. (They may take a while to run). You are free to modify or implement your own code.
def greedy(k):

  # list of unique user id
  users = data.userid.unique()

  # initialize the list of candidate users
  greedy = []

  # initialize the time tracker
  time_k = 0

  # loop over the cardinality values
  for i in range(k):

    start_time = time.time()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Find the user who contributes the most gain with respect to the 
    # objective value
    # Save this user in a variable `candidate_user` 
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # YOUR CODE HERE 

    # update the list of candidate users
    greedy = greedy + [candidate_user]

    end_time = time.time() - start_time
    time_k += end_time

    logging.info('k = {}'.format(i+1))
    logging.info("The set of users: {}".format(greedy))
    logging.info("Objective value = {}".format(F(greedy)))
    logging.info("Time taken = {} seconds".format(round(time_k, 5)))

    # -------------------------------------------
    # remove the chosen candidate from user list
    #-------------------------------------------
    # YOUR CODE HERE


# Q2: Lazy Greedy Algorithm
# Now, revise the code by implementing a lazy greedy optimization algorithm. Report the set of users and the objective values for k = 7 and 8.
# Hints: (1) The results should be the same for all k between the two algorithms. (2) The objective value for k = 5 is 18200. (3) It may be useful, but not required, to use the heapq algorithm in keeping the sorted array.
logging.info('Q2: Lazy Greedy Algorithm')
def lazy(k): 

  # list of unique user id
  users = data.userid.unique()

  # initialize the list of candidate users
  lazy_greedy = []

  # ---------------------------------------------------------
  # You may want to initialize a list of marginal gains
  # --------------------------------------------------------
  # YOUR CODE HERE

  
  # initialize the time tracker
  time_k = 0
  
  # loop over the cardinality value
  for i in range(k):
    start_time = time.time()

    if i == 0:
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Find the user who contributes the most gain with respect to the 
        # objective value, 
        # pop and save this user in a variable `candidate_user', 
        # keep the rest in a sorted array with repect to their marginal gains 
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # YOUR CODE HERE 
        pass

    else:  
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # This should include a while loop, where a comparison between the second
        # best candidate and the third best candidate is made. 
        # Once you find the next candidate, pop and save this user in `candidate_user' 
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # YOUR CODE HERE 
        pass

    lazy_greedy.append(candidate_user)

    end_time = time.time() - start_time
    time_k += end_time 

    logging.info('k = {}'.format(i+1))
    logging.info("The set of users: {}".format(lazy_greedy))
    logging.info("Objective value = {}".format(F(lazy_greedy)))
    logging.info("Time taken = {} seconds".format(round(time_k, 5)))
    