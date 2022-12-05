# Q1 Information and Honor Code (0 Points)
In this assignment, you complete the Colab3 notebook and obtain results from it. Your answer would be an integer, or a float number. The float value should be a decimal number rounded to the nearest 0.001. For example, 0.2435 would become 0.244. 

You can submit as many times as you want, and the last submission will be graded. No late day is allowed for any Colab assignment. 

Please verify that you have read the above instructions and the Honor Code and that you have not given or received unpermitted aid while completing this assignment.

If you have any questions about how the Honor Code applies to Colab assignments or other parts of the course, please contact the teaching staff for clarification.

# Q2 Collaborative Filtering (7 Points)

## Q2.1 Movie size (1 Point)
How many movies are in the dataset? (Integer)

Answer:

## Q2.2 Prediction error - Model 1 (1 Point)
Use Spark collaborative filtering (maximal iteration 10, rank 10, regularization 0.1, and drop rows with a NaN value in rating), and train your model on the training set. 

What is the root-mean-square error (RMSE) for predicting the movie ratings on the test set? (Float. The answer might vary due to the training process, and we will accept answer +/- 0.004 from the reference.)

Answer: 

## Q2.3 Prediction error - Model 2 (1 Point)
Next, you train a new model with different rank, while keeping the other parameters the same. 

With rank 5, what is the RMSE for predicting the movie ratings on the test set? (Float. The answer might vary due to the training process, and we will accept answer +/- 0.003 from the reference. )

Answer: 

## Q2.4 Prediction error - Model 3 (1 Point)
Then you train a new model with different rank, while keeping the other parameters the same. 

With rank 100, what is the RMSE for predicting the movie ratings on the test set? (Float. The answer might vary due to the training process, and we will accept answer +/- 0.002 from the reference. )

Answer: 

## Q2.5 Prediction error - Regularization (1 Point)
You further test different regularizations (1, 0.3, 0.1, 0.03, 0.01) with rank 100. Which one of the regularizations gives the lowest RMSE for predicting the movie ratings on the test set?

A: 1
B: 0.3
C: 0.1
D: 0.03
E: 0.01

Answer: 

# Q2.6 Recommendation- Top 1 (2 Points)
Use the collaborative filtering model trained with rank 100 and regularization 0.1. 

Now you want to make some recommendations. Instead of examining individual users, we want to recommend a single movie to all users.

First, we generate the top-1 recommendation for each user in the dataset.

Second, we count the number of times a movie is recommended, and identify the movie that is recommended to the largest number of users. 

Which is the most recommended movie to all users output by your model?

A: Someone Else's America (1995)
B: Titanic (1997)
C: Schindler's List (1993)
D: Pather Panchali (1955)
E: Angel Baby (1995)

Answer:

# Survey: 
## Approximately how long did you spend on this exercise?

Your answer:

## Level of effort - please rate Colab 1 on basis of how hard you found it.

Much too easy
Somewhat too easy
About right
Somewhat too hard
Much too hard

Your answer: 

## Contribution to learning - please rate Colab 1 on basis of how valuable you found it in advancing your learning?

Not at all useful
Slightly useful
Somewhat useful
Very useful
Extremely useful

Your answer:

## Suggestions for Colab 1 improvements

Your answer: