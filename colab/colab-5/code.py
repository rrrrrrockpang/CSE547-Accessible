# CSE547 - Colab 5
# PageRank
# Adapted from Stanford's CS 246

# Setup
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/code.log", "w+"),
                            logging.StreamHandler()
                    ]
)

# Data Loading
# For this Colab we will be using [NetworkX](https://networkx.org/), a Python package for the creation, manipulation, and study of the structure, dynamics, and functions of complex networks.
# If you want to explore NetworkX more, consider following this [tutorial](https://networkx.org/documentation/stable/tutorial.html), although it isn't necessary for this Colab.
# The dataset we will analyze is a snapshot of the Web Graph centered around stanford.edu, collected in 2002. Nodes represent pages from Stanford University (stanford.edu) and directed edges represent hyperlinks between them. [More Info](http://snap.stanford.edu/data/web-Stanford.html)
import networkx as nx

G = nx.read_edgelist('./data/web-Stanford.txt', create_using=nx.DiGraph)
print(nx.info(G))
logging.info("Step 1: Print nx.info(G): {}".format(nx.info(G)))

# Your Task
# To begin with, let's simplify our analysis by ignoring the dangling nodes and the disconnected components in the original graph.
# Use NetworkX to identify the largest weakly connected component in the G graph. From now on, use this connected component for all the following tasks.
# Functions you'll probably need to use:
# nx.weakly_connected_components (URL: https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.weakly_connected_components.html)
# G.subgraph (URL: https://networkx.org/documentation/stable/reference/classes/generated/networkx.Graph.subgraph.html)
# YOUR CODE HERE


# Compute the PageRank vector, using the default parameters in NetworkX: pagerank 
# Functions you'll probably need to use:
# nx.pagerank (URL: https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.link_analysis.pagerank_alg.pagerank.html#networkx.algorithms.link_analysis.pagerank_alg.pagerank)
# YOUR CODE HERE


# In 1999, Barabási and Albert proposed an elegant mathematical model which can generate graphs with topological properties similar to the Web Graph (also called Scale-free Networks).
# If you complete the steps below, you should obtain some empirical evidence that the Random Graph model is inferior compared to the Barabási–Albert model when it comes to generating a graph resembling the World Wide Web!
# As such, we will use two different graph generator methods, and then we will test how well they approximate the Web Graph structure by means of comparing the respective PageRank vectors. [NetworkX Graph generators]
# Using for both methods seed = 1, generate:
# 1. a random graph (with the fast method), setting n equal to the number of nodes in the original connected component, and p = 0.00008
# 2. a Barabasi-Albert graph (with the standard method), setting n equal to the number of nodes in the original connected component, and finding the right integer value for m such as the resulting number of edges approximates by excess the number of edges in the original connected component
# and compute the PageRank vectors for both graphs.

# Functions you'll probably need to use:
# nx.pagerank (URL: https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.link_analysis.pagerank_alg.pagerank.html#networkx.algorithms.link_analysis.pagerank_alg.pagerank)
# nx.fast_gnp_random_graph (URL: https://networkx.org/documentation/stable/reference/generated/networkx.generators.random_graphs.fast_gnp_random_graph.html#networkx.generators.random_graphs.fast_gnp_random_graph)
# nx.barabasi_albert_graph (URL: https://networkx.org/documentation/stable/reference/generated/networkx.generators.random_graphs.barabasi_albert_graph.html#networkx.generators.random_graphs.barabasi_albert_graph)
# YOUR CODE HERE

# Compare the PageRank vectors obtained on the generated graphs with the PageRank vector you computed on the original connected component. Sort the components of each vector by value, and use cosine similarity as similarity measure.
# Feel free to use any implementation of the cosine similarity available in third-party libraries, or implement your own with numpy.
# YOUR CODE HERE

# Once you have working code for each cell above, head over to Gradescope, read carefully the questions, and submit your solution for this Colab!