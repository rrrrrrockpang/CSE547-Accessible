# CSEP 590A - Colab 9
# Causal Inference
# Setup
# we import some of the common libraries for our task.
import logging 

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/code.log", "w+"),
                            logging.StreamHandler()
                    ]
)

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

import statsmodels.api as sm
from statsmodels.api import OLS
from sklearn.neighbors import NearestNeighbors

# Your Task
# In this colab, you will work on a fictitious data that aims to demonstrate some of the common challenges and mistakes in estimating the Average Treatment Effects (ATE). Below is the description of the fictitious data:
# SuperShoes Company recently released a new product named Lightning Shoes, and claimed that wearing the shoes will significantly speed up how fast a person can run. A skeptical researcher decided to conduct a study to test the credibility of this claim. To do this, he recruited 20,000 people who may or may not have bought the lightning shoes, and asked them to record how many meters they can sprint in one minute. For those who have bought the lightning shoes, they would need to wear the shoes to do the test run. The researcher also collected a variety of data on the participants' demographic and health-related information. The final sample the researcher obtained is the running.csv dataset, with the following variables:
# * speed: number of meters the participant runs in a minute.
# * lightning: whether the participant wore the lightning shoes for the run. 1 = yes, 0 = no.
# * age: age of the participant.
# * gender: 1 = male, 0 = female.
# * veg: 1 = vegetarian, 0 = non-vegetarian.
# * exercise: exercise level on a scale of [1, 10]
# * muscle: muscle level on a scale of [1, 10]
# * stress: stress level on a scale of [1, 10]
# * heart: heart rate that is recorded after the 1 minute run.
# We are interested in estimating the treatment effect of wearing the lightning shoes (i.e. the cause) on the speed people can run (i.e. the outcome).

# Data Overview
# load the data
df = pd.read_csv("./data/running.csv")
# have a view on the data 
logging.info("Data Overview: df.describe(): {}".format(df.describe()))

# Q1: Calculate the unadjusted ATE.
# First, calculate the difference in the average speed between people who wear the lightning shoes (i.e. treatment group) and people do not (i.e. control group). Let's call this the unadjusted ATE. Report the unadjusted ATE rounded to 3 decimal places.
# YOUR CODE HERE

# We can obtain the same result by running a simple linear regression where the independent variable is the treatment status (i.e. whether the participant wears the lightning shoes or not), and the outcome variable is the speed of running. The OLS_estimate() below uses the [statsmodels API](https://www.statsmodels.org/stable/index.html) to perform the linear regressions. Check whether the coefficient on lightning is the same as what you have obtained in Q1.
def OLS_estimate(outcome, covariates):
    covariates = sm.add_constant(covariates) # adding a constant 
    model = OLS(outcome, covariates)
    result = model.fit()
    print(result.summary())

ols_estimate = OLS_estimate(df['speed'], df[['lightning']])
logging.info("OLS Estimate: {}".format(ols_estimate))

# The result so far shows that there is a statistically significant yet relatively small effect of wearing the shoes, but the unadjusted ATE is negative, suggesting the shoes make you slower! However, this result is most likely biased. In fact, the true effect is known to be 10. What has gone wrong?

# The main problem here is that people who choose to obtain the shoes are different from people who choose not to obtain the lightning shoes. This is called the selection bias, where people select themselves into the treatment group due to some other factors that also affect how fast they can run. Those factors are called the confounders. To see this, let's calculate the sample averages of the covariates between the two groups.
agg_mean = df.groupby('lightning')['exercise', 'age', 'stress', 'gender', 'muscle', 'veg', 'heart'].agg(['mean'])
logging.info("Agg Mean: {}".format(agg_mean))
# This shows that people in the treatment group are on average older, exercise less, and have less muscle. In the next section, we control for those variables and see how they change our estimates.

# Q2: Linear Regression
# * In this section, run a set of linear regressions using the OLS_estimate() function, with the following additional covariates besides the lightning variable:
# Specification 1 (S1): exercise, age, gender, veg.
# Specification 2 (S2): include all variables.
# Specification 3 (S3): exercise, age, gender, stress, muscle.
# Q2: What are the estimated ATEs from these three specifications?
# YOUR CODE HERE

# As you can see from the results, apart from S3, which is our true model, S1 and S2 both severely under-estimate the true effect of 10, even though the coefficeints are both statiscally significant (i.e. with extremely small p-value). This demonstrates the fact that even if the estimates are statistically significant and the R-squared score is nearly perfect, as in this case, it does not indicate we have obtained the unbiased estimates.

# This also demonstrates that including more variables do not necessarily reduce our biases. In S1, the result is under-estimated because we fail to control for some of the confounders such as stress and muscle. However, controlling for too many variables, such as in S2, is also wrong. What happened in S2 is that we have a collider in the data, where the heart rate is affected by both the outcome (i.e. speed) and the treatment variable (i.e. lightning). Controlling for the collider would lead to distorted associations between the treatment and outcome. For more details on this problem, see [collider](https://en.wikipedia.org/wiki/Collider_(statistics))).

# Q3: Propensity Score Matching
# One method to tackle the problem is propensity score matching (PSM). The idea is to reduce the biases by comparing outcomes between the treated and control subjects who have similar propensity to be treated (i.e. propensity scores). In this section, you will implement three types of matching: (1) nearest-neighbor matching (2) stratification matching, and (3) inverse probability weighting (IPW) matching.
# The gen_pscore() function generates the propensity scores by performing a logistic regression. It also plots the distributions of generated propensity scores across the groups. We will use this function to generate the propensity scores using exercise, age, stress, gender, muscle and veg as covariates.
def gen_pscore(data, outcome, covariates):
    model = sm.Logit(outcome, covariates)
    result = model.fit()
    data['pscore'] = result.predict(covariates)
    data.groupby(['lightning']).pscore.plot(kind='hist', bins=20, alpha=0.8, legend=True)

gen_pscore(df, df['lightning'], df[[ 'exercise', 'age', 'stress', 'gender', 'muscle', 'veg']] )

# The table below summarizes the distribution of the propensity scores for the two groups respectively. 
df.groupby(['lightning']).pscore.describe()
logging.info("Propensity Score: {}".format(df.groupby(['lightning']).pscore.describe()))

# Notice that some scores lie outside of the common ranges. The trim() function below trims the sample such that the treatment and control group share the common support.
def trim(data):
  control_data = data[data.lightning == 0]
  treat_data = data[data.lightning == 1]

  min_control, min_treat = control_data.pscore.min(), treat_data.pscore.min()
  max_control, max_treat = control_data.pscore.max(), treat_data.pscore.max()

  min_support = max(min_control, min_treat)
  max_support = min(max_control, max_treat)

  trim_data = data.loc[((data.pscore >= min_support) & (data.pscore <= max_support)),:]
  
  return trim_data

trim_df = trim(df)
trim_df.groupby(['lightning']).pscore.describe()

## Q3: Perform Nearest-Neighbor Matching and report the ATE estimate with 3 decimal places.
# The function Nearest_Neighbor_Pair() below finds one control unit that has the nearest propensity score for each treatment unit (with replacement), and returns a dataframe that contains only the control units that are matched.
# Use this function to compute the average treatment effect by calculating the mean of the differences in outcomes between the treated and controlled for each pair. Remember to use the trimmed data.
def Nearest_Neighbor_Pair(treated_df, non_treated_df):
    treated_x = treated_df['pscore'].values
    non_treated_x = non_treated_df['pscore'].values

    nbrs = NearestNeighbors(n_neighbors=1).fit(np.expand_dims(non_treated_x, axis=1))
    distances, indices = nbrs.kneighbors(np.expand_dims(treated_x, axis=1))
    indices = indices.reshape(indices.shape[0])
    matched = non_treated_df.iloc[indices]
    return matched
# YOUR CODE HERE

# Note that what we have obtained is actually the Average Treatment Effect on the Treated (ATT), since we are matching on the treated units. By doing so, we are implicitly assuming that the control unit we have picked as counterpart for each treated unit is a good approximation of what the outcome of the treated unit would be if it were not treated.
# In fact, the ATT does not necessarily have to be the same as ATE or ATU (the average treatment effect on the untreated). Since the true effect is constant across all the samples (i.e. constant treatment effect), the ATT is equal to ATE in this particular case.

# Q4: Perform stratification matching and report the estimate with 3 decimal places.
# Perform a stratification matching with 10 equally spaced stratas, where strata = 1 if pscore is in (0, 0.1], strata = 2 if pscore is in (0,1, 0.2] and so on. Report the average treatment effect on the treated (ATT) using the following formula:
# ATT_{strata} = \sum_{s=1}^{s=10} N_s^T/N^T * (\bar{Y_s}^T - \bar{Y_s}^C)
# where N^T is the number of treated in the whole sample,  N_s^T is the number of treated units in strata s, \bar{Y_s}^T - \bar{Y_s}^C is the difference in sample averages of outcomes between the treated and control within strata  𝑠 . Remember to use the trimmed data.
# YOUR CODE HERE

# Q5: Perform Inverse Probability Weighted (IPW) Matching and report the estimate in 3 decimal places.
# We can also estimate the treament effect using the Inverse Probability Weighted (IPW) Matching method. The ATT from IPW matching is calculated as follows:
# ATT_{IPW} = 1/n \sum_i \frac{T_i Y_i}{p_i} - 1/n \sum_i  \frac{(1-T_i) Y_i}{1-p_i}
# where T_i is 1 if i is treated, and 0 if not. Y_i is the outcome of individual i and p_i is the propensity score for individual i.
# Compute the ATT from IPW matching.
# YOUR CODE HERE

# In practice, you do not need to do all the estimations from scratch. There are various libraries/packages that implement these methods for you. For instance, check out the [causalinference package](https://causalinferenceinpython.org/causalinference.html#module-causalinference.causal) if you are interested.
# Once you obtained the desired results, head over to Gradescope and submit your solution for this Colab!


