# Code to create the network using NetworkX.

import networkx as nx

def create_network(data):
    G = nx.DiGraph()
    for _, row in data.iterrows():
        G.add_edge(row['source'], row['target'])
    return G

