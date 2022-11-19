# Association Rules (45 points): This is a math question. You can answer them below. 

Association Rules are frequently used for Market Basket Analysis (MBA) by retailers to understand the purchase behavior of their customers. This information can be then used for many different purposes such as cross-selling and up-selling of products, sales promotions, loyalty programs, store design, discount plans and many others.

In this question, you will investigate the association rules in more detail which can enable you to select more informative and suitable rules for various recommendation systems in the future.

## Evaluation of item sets: 

Once you have found the frequent itemsets of a dataset, you need to choose a subset of them as your recommendations. Commonly used metrics for measuring significance and interest for selecting rules for recommendations are:

1. **Confidence** (denoted as $\conf(A \rightarrow B)$): 

**Confidence** is defined as the probability of occurrence of $B$ in the basket if the basket already contains $A$:

\[
	\conf(A \rightarrow B) = \Pr(B|A),
\]

where $\Pr(B|A)$ is the conditional probability of finding item set $B$ given that item set $A$ is present. 

2. **Lift** (denoted as $\lift(A \rightarrow B)$):
**Lift** measures how much more ``$A$ and $B$ occur together'' than ``what would be expected if $A$ and $B$ were statistically independent'':

\[
	\lift(A \rightarrow B) = \frac{\conf(A \rightarrow B)}{S(B)},
\]

where $S(B) = \frac{\Support(B)}{N}$ and $N=\text{total number of transactions (baskets)}$.

3. **Conviction** (denoted as $\conv(A \rightarrow B)$): 
**Conviction** compares the ``probability that $A$ appears without $B$ if they were independent'' with the ``actual frequency of the appearance of $A$ without $B$'':
\[
	\conv(A \rightarrow B) = \frac{1-S(B)}{1-\conf(A \rightarrow B)}.
\]
\end{enumerate}

Answer the question here: 

\subquestion{(a) [4pts]}
A drawback of using **confidence** is that it ignores $\Pr(B)$. Why is this a drawback? Explain why **lift** and **conviction** do not suffer from this drawback.

\subquestion{(b) [5pts]}
A measure is \textit{symmetrical} if $\text{measure}(A \rightarrow B) = \text{measure}(B \rightarrow A)$. Which of the measures presented here are symmetrical?  For each measure, please provide either a proof that the measure is symmetrical, or a counterexample that shows the measure is not symmetrical.

\subquestion{(c) [6pts]}
\textit{Perfect implications} are rules that hold 100\% of the time (or equivalently, the associated conditional probability is 1). A measure is \textit{desirable} if it reaches its maximum achievable value for all perfect implications. This makes it easy to identify the best rules. Which of the above measures have this property? You may ignore $0/0$ but not other infinity cases. Also you may find it easy to explain by an example.

\subquestion{(d) [15pts]}
Identify pairs of items $(X,Y)$ such that the support of $\{X,Y\}$ is at least $100$ and list the top 5 pairs with the highest support in the writeup. For all such pairs (the support of $\{X,Y\}$ is at least $100$), compute the **confidence** scores of the corresponding association rules: $X \Rightarrow Y$, $Y \Rightarrow X$. Sort the rules in decreasing order of **confidence** scores and list the top 5 rules in the writeup. Break ties, if any, by lexicographically increasing order on the left hand side of the rule. 

