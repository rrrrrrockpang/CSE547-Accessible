# CSE547 - Colab 6
# node2vec
# Adapted from Stanford's CS246

# Setup
# First of all, we install the [nodevectors library](https://github.com/VHRanger/nodevectors) which offers a fast implementation of the node2vec method.
# If you are curious to learn how to implement fast random walks on graphs, I recommend you to read [the blog post](https://www.singlelunch.com/2019/08/01/700x-faster-node2vec-models-fastest-random-walks-on-a-graph/) which explains some of the design choices behind this library.

# We now import the library, and create a small wrapper class which will expose only the few hyperparameters we will need to tune in this exercise.
from nodevectors import Node2Vec
import networkx as nx
import logging
logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/intro.log", "w+"),
                            logging.StreamHandler()
                    ]
)

class Node2Vec(Node2Vec):
  """
  Parameters
  ----------
  p : float
      p parameter of node2vec
  q : float
      q parameter of node2vec
  d : int
      dimensionality of the embedding vectors
  """
  def __init__(self, p=1, q=1, d=32):
    super().__init__(
                     walklen=10,
                     epochs=50,
                     n_components=d,
                     return_weight=1.0/p,
                     neighbor_weight=1.0/q,
                     threads=0,
                     w2vparams={'window': 4,
                                'negative': 5, 
                                'iter': 10,
                                'ns_exponent': 0.5,
                                'batch_words': 128})


# Lastly, let's import some of the common libraries needed for our task.
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy

# Example
# In the example below, we will use Zachary's Karate Club to demonstrate the power of node2vec.
# Load the Zachary's Karate Club as a NetworkX Graph object
KCG = nx.karate_club_graph()

# Fit embedding model to the Karate Club graph
n2v = Node2Vec(1, 1, 2)
res = n2v.fit(KCG)
logging.info('Fitted node2vec model: n2v.fit(KCG): {}'.format(res))

embeddings = []
for node in KCG.nodes:
  embedding = list(n2v.predict(node))
  club = KCG.nodes[node]['club']
  embeddings.append(embedding + [club])

# Construct a pandas dataframe with the 2D embeddings from node2vec,
# plus the club name that each node belongs to after the split
df = pd.DataFrame(embeddings, columns=['x', 'y', 'club'])
logging.info('\t'+ df.to_string().replace('\n', '\n\t'))  # pretty print in logging

# Nodes who stayed with the Mr. Hi will be plotted in red, while nodes
# who moved with the Officer will be plotted in blue
colors = ['red' if x == 'Mr. Hi' else 'blue' for x in df.club]
df.plot.scatter(x='x', y='y', s=50, c=colors)
# Alt text: The resultig graph is a scatterplot, colored by blue and red. All the red dots are x > -0.6, y < -1.0. All the blue dots are x < -0.6, y > -1.0. The two groups are clearly separated, and the embedding is able to capture the split in the graph.
# If our example trained correctly, you should notice a clear separation between the blue and red nodes. Solely from the graph structure, node2vec could predict how the Zachary's Karate Club split!
# Tune the hyperparameters p and q, and notice how they affect the resulting embeddings.

# Your Task
# Now we will study the behavior of node2vec on [barbell graphs](https://en.wikipedia.org/wiki/Barbell_graph).
# Below you can see a toy example of a barbell graph generated with NetworkX ([nx.barbell_graph](https://networkx.org/documentation/stable/reference/generated/networkx.generators.classic.barbell_graph.html#networkx.generators.classic.barbell_graph)).
toy_barbell = nx.barbell_graph(7, 2)
nx.draw_kamada_kawai(toy_barbell)
# Alt text: The resultig graph is a barbell graph with 7 nodes on each side, and 2 nodes in the middle. The two sides are connected by a single edge.

# Above is a barbell graph with a 2 node path inbetween two complete graphs.
# Generate a larger barbell graph, where each complete graph has exactly 1000 nodes, and there are no nodes in the path inbetween them (i.e., all the nodes in the barbell graph belong to either one of the two complete graphs, and the connecting path does not have any internal node).
# Then, learn node2vec embeddings on this graph, setting p = 1, q = 1 and d = 10.
# YOUR CODE HERE

# Generate another barbell graph, this time adding a path of 50 nodes between the two complete graphs.
# Learn the node2vec embeddings for the nodes of this new graph, using the same hyperparameters as before.
# YOUR CODE HERE

# Now write a function that takes as input a node id n in the graph (e.g., 5) and returns a list containing the cosine similarity between the node2vec vector obtained from `model.predict(...)`` of the input node n and all the nodes in the given barbell graph (including the similarity with n itself).
# It may be easier to implement your own `cosine_similarity`` function, as using sklearn's `cosine_similarity`` function makes assumptions on the shape of the input and requires you to reshape the embeddings before use, which might be a little slow.
# YOUR CODE HERE

# For each of the graphs you generated earlier (0 Node Path, 50 Node Path), find how many nodes have exactly 1000 neighbors and how many nodes have less than 100 neighbors.
# Two nodes are defined as neighbors if their cosine similarity is greater than 0.8.
# YOUR CODE HERE

# Once you have working code for each cell above, head over to Gradescope, read carefully the questions, and submit your solution for this Colab!