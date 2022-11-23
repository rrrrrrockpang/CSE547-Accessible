# Locality-Sensitive Hashing (20 pts)

When simulating a random permutation of rows, as described in **Sect. 3.3.5** of MMDS, we could save time if we restricted our attention to a randomly chosen $k$ of the $n$ rows, rather than hashing all $n$ row numbers.  The downside of doing so is that, if none of the $k$ rows contains a $1$ in a certain column, then the result of the minhashing is "don't know". In other words, we get no row number as the minhash value.  It would be a mistake to assume that two columns that both minhash to "don't know" are likely to be similar.  However, if the probability of getting "don't know" as a minhash value is small, we can tolerate the situation and simply ignore such minhash values when computing the fraction of minhashes in which two columns agree. 

You will try to understand the process of selecting an appropriate $k$ value and how it affects the computation of the minhash. This exercise will highlight the importance of parameter selection for LSH.

In part (a) we determine an upper bound on the probability of getting "don't know" as the minhash value when considering only a $k$-subset of the $n$ rows, and in part (b) we use this bound to determine an appropriate choice for $k$, given our tolerance for this probability.


* Question (a) [7pts]

Suppose a column has $m$ 1's and therefore $n-m$ 0's, and we randomly choose k rows to consider when computing the minhash.  Prove that the probability of getting "don't know" as the minhash value for this column is at most $\left(\frac{n-k}{n}\right)^m$ (Note: m is the power, not a multiplication).

* Question (b) [7pts]

Suppose we want the probability of "don't know" to be at most $e^{-10}$.  Assuming $n$ and $m$ are both very large (but $n$ is much larger than $m$ or $k$), give a simple approximation to the smallest value of $k$ that will ensure this probability is at most $e^{-10}$. Your expression should be a function of $n$ and $m$. Hints: (1) Part a. (2) Remember that for any $x \in \mathbb{R}$, $1 + x \le e^x$.

* Question (c) [6pts]

In LSH, there are relationships between different similarity metrics and hash functions. In this question, we will go over an example where the similarity metric and the hash function do not match.

Note: Part (c) should be considered separate from the previous two parts, in that we are no longer restricting our attention to a randomly chosen subset of the rows.

When minhashing, one might expect that we could estimate the Jaccard similarity without using all possible permutations of rows. For example, we could only allow cyclic permutations, i.e. start at a randomly chosen row $r$, which becomes the first in the order, followed by rows $r + 1$, $r + 2$, and so on, down to the last row, and then continuing with the first row, second row, and so on, down to row $r - 1$. There are only $n$ such permutations if there are $n$ rows. However, these permutations are not sufficient to estimate the Jaccard similarity correctly.

Give an example of two columns such that the probability (over cyclic permutations only) that their minhash values agree is not the same as their Jaccard similarity. In your answer, please provide (a) an example of a matrix with two columns (let the two columns correspond to sets denoted by $S1$ and $S2$), (b) the Jaccard similarity of $S1$ and $S2$, and (c) the probability that a random cyclic permutation yields the same minhash value for both $S1$ and $S2$.

## What to submit

Include the following in your writeup:

- Proof for 3(a)
- Derivation and final answer for 3(b)
- Example for 3(c) including the three requested items