import networkx as nx

from .utils import *
import os


class NetworkCombiner(nx.DiGraph):
    """It combines two networks of 'judgments' and 'decisions'"""
    def __init__(self, graph_directory, judgment_graph, decision_graph):
        """
        Initializes the class.

        Parameters
        ----------
        file_path : str, optional
            file path to the judgments and decisions networks.
        judgment_graph: str
            the filename of the first graph
        decision_graph: str
            the filename of the second graph
        """
        super().__init__()
        self.graph = nx.DiGraph()

        judgment_graph_address = os.path.join(graph_directory, judgment_graph)
        decision_graph_address = os.path.join(graph_directory, decision_graph)
        self.graph_judgments = self.load_graph(judgment_graph_address)
        self.graph_decisions = self.load_graph(decision_graph_address)


    def load_graph(self, file_path):
        return load_graph_from_json(file_path)

    @staticmethod
    def merge_judgment_and_decision_nodes(G_judgments, G_decisions):
        """
        Merge nodes from 'judgments' and 'decisions' networks into a new network.

        Returns:
        - nx.DiGraph: Merged network containing nodes from both 'judgments' and 'decisions'.
        """

        G_merged = nx.DiGraph()

        num_decision_to_judgment = num_judgment_to_decision = num_removed = 0
        num_judgment_and_decision = 0
        num_judgments_no_change = num_decisions_no_change = 0

        for node_judgment, data_judgment in G_judgments.nodes(data=True):
            if node_judgment in G_decisions.nodes():
                data_decision = G_decisions.nodes[node_judgment]

                # for both (in decision and in judgment network), we do not have meta data
                if len(data_judgment) == 1 and len(data_decision) == 1:
                    # If one is duplicate and the other is not, use the non-duplicate one
                    if data_judgment['label'] == 'Duplicate' and data_decision['label'] != 'Duplicate':
                        G_merged.add_node(node_judgment, **data_judgment)
                        num_decision_to_judgment += 1
                    elif data_judgment['label'] != 'Duplicate' and data_decision['label'] == 'Duplicate':
                        G_merged.add_node(node_judgment, **data_decision)
                        num_judgment_to_decision += 1
                    # If both are not duplicate (they are real cases), then we remove one of them.
                    elif data_judgment['label'] != 'Duplicate' and data_decision['label'] != 'Duplicate':
                        # Remove one of them
                        G_merged.add_node(node_judgment, **data_judgment)
                        num_removed += 1
                    else:
                        # Both are duplicate
                        G_merged.add_node(node_judgment + "_JUD", **data_judgment)
                        G_merged.add_node(node_judgment + "_DEC", **data_decision)
                        num_judgment_and_decision += 1
                # If judgment is duplicate and decision is not: keep decision node and remove the other
                elif len(data_judgment) == 1 and len(data_decision) > 1:
                    G_merged.add_node(node_judgment, **data_decision)
                    num_judgment_to_decision += 1
                # If decision is duplicate and judgment is not: keep decision node and remove the other
                elif len(data_judgment) > 1 and len(data_decision) == 1:
                    G_merged.add_node(node_judgment, **data_judgment)
                    num_decision_to_judgment += 1
                # If both are not duplicate, and represent real case laws (one judgment and one decision)
                else:
                    # Add two nodes: One node for decision, and one node for judgment
                    G_merged.add_node(node_judgment + "_JUD", **data_judgment)
                    G_merged.add_node(node_judgment + "_DEC", **data_decision)
                    num_judgment_and_decision += 1
                    print("This case (%s) is present for both decisions and judgments with meta-data" % node_judgment)
            # If we have a judgment (and there is no decision with the same application number and judgment year)
            else:
                G_merged.add_node(node_judgment, **data_judgment)
                num_judgments_no_change += 1

        # adding decision nodes
        for node_decision, data_decision in G_decisions.nodes(data=True):
            if node_decision not in G_judgments.nodes():
                G_merged.add_node(node_decision, **data_decision)
                num_decisions_no_change += 1

        assert len(G_merged.nodes()) == (num_judgments_no_change + num_decisions_no_change +
                                         num_decision_to_judgment + num_judgment_to_decision +
                                         num_removed + 2 * num_judgment_and_decision)

        print(f"{num_decision_to_judgment} decision nodes were merged into judgments.")
        print(f"{num_judgment_to_decision} judgment nodes were merged into decisions.")
        print(
            f"{num_removed} decision nodes were removed (there is a judgment with the same appno. and they are both duplicate).")
        print(f"{num_judgment_and_decision} cases of decisions and judgments have the same app. no. (without features).")

        with_meta_data = [p for p, d in G_merged.nodes(data=True) if len(d) > 1]
        print("\nThere are %i cases with meta-data" % len(with_meta_data))
        print("There are %i cases without meta-data" % (len(G_merged.nodes()) - len(with_meta_data)))

        # save_graph_to_json(G_merged, fname='graph_JUD_DEC')

        return G_merged

    @staticmethod
    def add_judgment_and_decision_edges(graph, judgment_graph, decision_graph):
        """
        Add citations to the combined 'judgments' and 'decisions' network.

        This function integrates the edges from two separate graphs (judgments and decisions) into a unified graph.

        Parameters:
        ----------
        graph : networkx.DiGraph
            The combined graph consisting of 'judgments' and 'decisions' nodes.

        Returns:
        -------
        networkx.DiGraph
            The updated graph with added citation edges.
        """
        # Load the judgment and decision graphs
        # judgment_graph = load_graph_from_json(fname='data/graphJUD')
        # decision_graph = load_graph_from_json(fname='data/graphDEC')

        # G_jud = load_graph_from_json(fname='data/graphJUD')
        # G_dec = load_graph_from_json(fname='data/graphDEC')

        for node in graph.nodes():
            if node.endswith("_JUD"):
                # Node related to judgments
                base_node = node[:-4]
                for _, target, data in judgment_graph.out_edges(base_node, data=True):
                    target_node = target if target in graph.nodes() else f"{target}_JUD"
                    assert target_node in graph.nodes(), f"The node {target} (and {target}_JUD) is not in the graph"
                    graph.add_edge(node, target_node, **data)
            elif node.endswith("_DEC"):
                # Node related to decisions
                base_node = node[:-4]
                for _, target, data in decision_graph.out_edges(base_node, data=True):
                    target_node = target if target in graph.nodes() else f"{target}_DEC"
                    assert target_node in graph.nodes(), f"The node {target} (and {target}_DEC) is not in the graph"
                    graph.add_edge(node, target_node, **data)
            else: # Node is unique (either judgments or decisions)
                # node is present in the judgment graph, but not in the decision graph
                if (node in judgment_graph.nodes()) and (node not in decision_graph.nodes()):
                    for _, target, data in judgment_graph.out_edges(node, data=True):
                        target_node = target if target in graph.nodes() else f"{target}_JUD"
                        assert target_node in graph.nodes(), f"The node {target} (and {target}_JUD) is not in the graph"
                        graph.add_edge(node, target_node, **data)
                # node is present in the decision graph, but not in the judgment graph
                elif (node in decision_graph.nodes()) and (node not in judgment_graph.nodes()):
                    for _, target, data in decision_graph.out_edges(node, data=True):
                        target_node = target if target in graph.nodes() else f"{target}_DEC"
                        assert target_node in graph.nodes(), f"The node {target} (and {target}_DEC) is not in the graph"
                        graph.add_edge(node, target_node, **data)
                # node is present in both judgment and decision networks
                elif (node in judgment_graph.nodes()) and (node in decision_graph.nodes()):
                    for _, target, data in judgment_graph.out_edges(node, data=True):
                        target_node = target if target in graph.nodes() else f"{target}_JUD"
                        assert target_node in graph.nodes(), f"The node {target} (and {target}_JUD) is not in the graph"
                        graph.add_edge(node, target_node, **data)
                    for _, target, data in decision_graph.out_edges(node, data=True):
                        target_node = target if target in graph.nodes() else f"{target}_DEC"
                        assert target_node in graph.nodes(), f"The node {target} (and {target}_DEC) is not in the graph"
                        graph.add_edge(node, target_node, **data)
                else:
                    assert True, f"Node {node} is not found in both judgments and decisions"

                if (node in judgment_graph.nodes()) and (node in decision_graph.nodes()):
                    if len(judgment_graph.nodes[node]) != 1 and len(decision_graph.nodes[node]) > 1:
                        assert True, f"Node {node} is found in both judgments and decisions (but they are saved without _JUD and _DEC)"

        return graph

    @staticmethod
    def remove_duplicate_nodes(G: nx.DiGraph):
        """
        Remove nodes labeled as 'Duplicate' from the graph.

        This function will:
        - Verify that duplicate nodes have exactly one outgoing edge and no incoming edges.
        - Remove the duplicate node from the graph.

        Parameters:
        ----------
        G : networkx.DiGraph
            The graph from which duplicate nodes are to be removed.

        Returns:
        -------
        networkx.DiGraph
            The graph after removing duplicate nodes.
        """

        print(f"there are {len(G.nodes())} nodes and {len(G.edges())} edges, before removing duplicates.")

        nodes_to_remove = []
        for node, data in G.nodes(data=True):
            if data['label'] == 'Duplicate':
                assert G.out_degree(
                    node) == 1, f"Node {node} (a duplicate case) has {G.out_degree(node)} outgoing citations."
                assert G.in_degree(
                    node) == 0, f"Node {node} (a duplicate case) has {G.in_degree(node)} incoming citations."
                # Identify the node the duplicate points to
                target_node = [n for (_, n) in G.out_edges(node)][0]

                # Remove the outgoing edge and the duplicate node
                G.remove_edge(node, target_node)
                nodes_to_remove.append(node)

                # out_neighbors = [n for (_, n) in G.out_edges(node)]
                # assert len(out_neighbors)==1, "node %s (a duplicate case) has %i outgoing citations." % (node, len(out_neighbors))
                # pointAt = [n for (_, n) in G.out_edges(node)][0]
                # assert G.nodes[pointAt]['label']=='Case', "node %s (a duplicate case) point to %s (which is not case but a %s)" % (node, pointAt, G.nodes[pointAt]['label'])
                #
                # l = [(node2, data2) for (node2, _, data2) in G.in_edges(node, data=True)]# if (node2, pointAt) not in G.edges()]
                # for (n, d) in l:
                #     if (n, pointAt) not in G.edges():
                #         G.add_edge(n, pointAt, **d)
                #     G.remove_edge(n, node)
                # assert len(G.in_edges(node))==0, 'there are still some incoming citation to the duplicated case %s' % node

        G.remove_nodes_from(nodes_to_remove)

        print(f"After removing duplicates: {len(G.nodes())} nodes and {len(G.edges())} edges.")
        return G

    def do_concatenation(self):
        """
        Concatenate the judgment and decision graphs by removing duplicate nodes, merging nodes, and adding citation edges.

        This method performs the following steps:
        1. Removes duplicate nodes from both the judgment and decision graphs.
        2. Merges the nodes of the judgment and decision graphs into a combined graph.
        3. Adds citation edges to the combined graph.

        Returns:
        -------
        networkx.DiGraph
            The combined graph with merged nodes and added citation edges.
        """
        # Remove duplicate nodes from both networks
        print("1) remove the duplicate cases for both networks.")
        self.remove_duplicate_nodes(self.graph_judgments)
        self.remove_duplicate_nodes(self.graph_decisions)

        # Create the combined network's nodes
        print("2) concatenate nodes of two networks")
        self.graph = self.merge_judgment_and_decision_nodes(self.graph_judgments, self.graph_decisions)

        # Add citations to the combined network
        print("3) adding edges to the combined network")
        self.add_judgment_and_decision_edges(self.graph, self.graph_judgments, self.graph_decisions)

        return self.graph

