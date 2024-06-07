import networkx as nx
from networkx.readwrite import json_graph

import pandas as pd
import json

import re

import collections

from .utils import *


class CitationNetwork(nx.DiGraph):
    """
        A class to represent and manage a citation network using NetworkX's DiGraph.

        Attributes
        ----------
        G : nx.DiGraph
            The directed graph representing the citation network.
        duplicate_symbol : str
            Symbol to denote duplicate nodes.
        window_size : int
            The window size for searching the year of a cited case in the text.
        category : str
            Category of the citation network.
        dataname : str
            Name of the dataset.
        evict_list : List[str]
            List of cases to be evicted.

        Methods
        -------
        partially_match(s1, s2, citation_year=3000, w=4)
            Checks if two strings partially match based on the given conditions.
        fully_match(s1, s2)
            Checks if two strings fully match.
        matching(s1, s2, citation_year=3000)
            Determines the level of matching between two strings.
        does_one_of_them_exist(nodes)
            Checks if at least one node from the given list exists in the graph.
        find_unique_id(node_id)
            Finds the unique ID of a node, handling duplicates.
        find_date(s, appno, citation_year)
            Extracts a valid date from a string based on the application number and citation year.
        merge_appno_year(date, delimiter=";")
            Merges application numbers and years into a single string with a delimiter.
        merge_appno_year_with_search(df, delimiter=";")
            Merges application numbers and years with a search in the text.
        add_cases_no_citation(appnos_year, delimiter=";", node_type='Case')
            Adds cases without citations to the graph.
        does_a_node_exist(new_node)
            Checks if a node exists in the graph.
        is_it_a_self_loop(source, target)
            Checks if an edge would create a self-loop.
        add_case_to_case_citations(series, delimiter=";", node_type='Case', edge_type="CASE_TO_CASE")
            Adds case-to-case citations to the graph.
        merge_cases_with_DAG_check()
            Merges cases while ensuring the graph remains a DAG.
        merge_cases()
            Merges cases based on application numbers.
        transfer_attr(dict1, dict2)
            Transfers attributes from one dictionary to another.
        transfer_edges(u, v, self_loops=False)
            Transfers edges from one node to another.
        set_attr_of_cases(node_ids, attrs)
            Sets attributes of cases in the graph.
        df2network(df, attrs)
            Converts a dataframe to a citation network.
        """
    def __init__(self):
        """
        Initializes the CitationNetworks class.

        Parameters
        ----------
        category : str, optional
            Category of the citation network (Judgment or decision): default is "JUD".
        """
        super().__init__()
        self.G = nx.DiGraph()
        self.duplicate_symbol = 'Duplicate'
        self.window_size = 100 # the window size for searching year of a cited case in the text
        # self.category = category

    @staticmethod
    def match_nodes_names(node1: str, node2: str, citation_year=3000):
        matching_nodes_names(node1, node2, citation_year)

    def find_unique_id(self, node_id: str) -> str:
        """
        Finds the unique ID of a node, handling cases with several application numbers (i.e. duplicates).
        Note that a node of type duplicate can only point to one case law (non-duplicate node)

        Parameters
        ----------
        node_id : str
            Node ID.

        Returns
        -------
        str
            Unique node ID.
        """
        if self.G.nodes[node_id]['label'] == self.duplicate_symbol:
            neighbors = list(self.G.neighbors(node_id))
            assert len(neighbors) == 1, f"Node {node_id} of type {self.duplicate_symbol} points to more than 1 node."
            return neighbors[0]
        return node_id

    def add_nodes_of_network(self, appnos_year: pd.Series, delimiter=";", node_type='Case'):
        """
        Create nodes representing case laws without citations (edges).
        If a node has more than one application number, only the first application number represents the case law,
        while the rest are represented as duplicate nodes pointing (by edges of type 'DUPLICATE_TO_CASE') to
         the first application number.

        Parameters
        ----------
        appnos_year : pd.Series
            Series containing nodes' identifiers, which include both application numbers and the year of judgment.
        delimiter : str, optional
            Separator for application numbers in cases with more than one application number (default is ";").
        node_type : str, optional
            Type of the node, default is 'Case'. It has two possible values:
            'Case': representing a real case law,
            'Duplicate': representing the extra application numbers for cases with more than one application number.

        Returns
        -------
        None
        """
        for ids in appnos_year.dropna():
            if isinstance(ids, str) and len(ids) > 0:
                nodes = ids.split(delimiter)

                self.G.add_node(nodes[0], label=node_type)
                for node in nodes[1:]:
                    assert not isinstance(node, list), "Node should not be a list."
                    if node not in self.G.nodes():
                        self.G.add_node(node, label=self.duplicate_symbol)
                        self.G.add_edge(node, nodes[0],
                                        label=self.duplicate_symbol.upper() + '_TO_' + node_type.upper())

    def is_it_a_self_loop(self, source, target):
        """
        Determines if an edge from source to target is a self-loop.

        This function checks if the source and target nodes are the same node or case law,
        if there is an existing edge from the target to the source, or if the edge already exists
        from the source to the target.

        Parameters
        ----------
        source : str
            The source node.
        target : str
            The target node.

        Returns
        -------
        bool
            True if it is a self-loop, False otherwise.
        """
        # If source and target are the same node/case law
        if self.match_nodes_names(source, target):
            return True
        # if source[:-5] == target[:-5]:
        #     return True

        # If there is an edge from the target to the source
        for (t, s, d) in self.G.in_edges(source, data=True):
            if t[:-5] == target[:-5]:
                return True
        # for (t, s, d) in self.G.in_edges(source, data=True):
        #     if self.match_nodes_names(t, target):
        #         return True

        if (source, target) in self.G.out_edges(source):
            return True
        return False


    def add_citations_to_network(self, citations_year: pd.Series, delimiter=";", node_type='Case', edge_type="CASE_TO_CASE"):
        """
        Adds citations between case laws.

        Parameters
        ----------
        citations_year : pd.Series
            Series containing citations, mapping from the pd.Series.index to the corresponding cited case laws.
        delimiter : str, optional
            Separator for cited case laws (default is ";").
        node_type : str, optional
            Type of the nodes, default is 'Case'.
        edge_type : str, optional
            Type of edges between two nodes. It has two types:
            - 'CASE_TO_CASE': citation between two case laws.
            - 'DUPLICATE_TO_CASE': a duplicate application number (for cases with more than one application number)
              pointing to the corresponding node of the case law (first application number).

        Returns
        -------
        None
        """
        for Case, refs_year in citations_year.dropna().items():
            source = Case.split(delimiter)[0]
            citation_year = source[-4:]

            for ref_year in refs_year.split(delimiter):
                # We assume any application number has at least three integers before '/' and exactly two integers after '/'
                if len(re.findall(r"\d*(\d\d\d/\d\d)", ref_year)) > 0:
                    if ref_year not in self.G.nodes():
                        self.G.add_node(ref_year, label=node_type)

                    # Ensure that the citation points to the node representing the case law (its first application number)
                    # Note that in the citations list, we have all application numbers of a case with several application numbers
                    target = self.find_unique_id(ref_year)

                    # If the source and target have the same application number, do not add a new edge
                    if not self.is_it_a_self_loop(source, target):
                        self.G.add_edge(source, target, label=edge_type, citedAt=citation_year)

    def merge_cases_with_matching_years(G):
        """
        Merge nodes with missing year information with nodes having complete year information.

        If a node does not have its year information but has the same application number as another node with
        year information, the two nodes are merged. The node with complete year information is retained, and
        the edges of the node with missing year information are redirected to the node with complete year information.

        Returns
        -------
        None
        """
        nodes_without_year = [n for n in G.nodes() if n[-4:] == 'None']
        nodes_with_year = [n for n in G.nodes() if n[-4:] != 'None']
        num_of_matches = 0

        for node1 in nodes_without_year:
            # Find the earliest citation year among the incoming edges of the node without year information
            citation_years = [int(data['citedAt']) for (_, _, data) in G.in_edges(node1, data=True)]
            if len(citation_years) == 0:
                citation_year = 3000
            else:
                citation_year = min(citation_years)

            for node2 in nodes_with_year:
                if matching_nodes_names(node1, node2, citation_year):
                    # Update attributes of the node with year information using the attributes of the node without year information
                    G.nodes[node2].update(concatenate_dict(G.nodes[node2], G.nodes[node1]))

                    # Transfer edges from the node without year information to the node with year information
                    new_edges = redirect_edges(node1, node2)
                    G.remove_node(node1)
                    G.add_edges_from(new_edges)

                    assert 'label' in G.nodes[node2], f'{node2} does not have a label.'
                    num_of_matches += 1
                    break

        print(f"We merged {num_of_matches} application numbers (out of {len(nodes_without_year)})")

    def concatenate_nodes_with_matching_years(self):
        merge_cases_with_matching_years(self.G)

    def set_node_attributes_from_dataframe(self, node_ids: pd.Series, attributes_df: pd.DataFrame):
        set_node_attributes_from_dataframe(self.G, node_ids, attributes_df)

    def remove_dictations_to_duplicates(self):
        edges_to_remove = get_citations_to_duplicates(self.G)
        print(f"\tThere are {len(edges_to_remove)} citation (to replicates) to remove.")

        edges_to_add = []
        for (s, dup) in edges_to_remove:
            target = self.find_unique_id(dup)
            assert self.G.nodes[target][
                       'label'] == 'Case', f"The target {target} should be of type 'Case', but it is of type {self.G.nodes[target]['label']}."
            if (s, target) not in self.G.edges():
                edges_to_add.append((s, target))

        self.G.remove_edges_from(edges_to_remove)
        self.G.add_edges_from(edges_to_add, label='CASE_TO_CASE')


    def dataframe_to_network(self, df, attrs: list):
        """
        Converts a DataFrame of case law data to a network representation.

        Parameters:
        - df (pd.DataFrame): DataFrame containing case law data.
        - attrs (list): List of attributes to be added to the network nodes.

        Returns:
        - pd.DataFrame: Processed DataFrame.
        """
        # Drop rows with missing application numbers and set index to 'AppNo'
        df.dropna(subset=['AppNo'], inplace=True)
        df.set_index('AppNo', inplace=True, drop=False)

        # Concatenate application number and year for easier comparison and remove duplicates
        df['AppNo_Year'] = concat_appno_year(df.JudgmentDate)
        df = df[~df.duplicated(subset=['AppNo_Year'])]
        df.set_index('AppNo_Year', inplace=True, drop=False)

        # Concatenate application number and year for references (it checks text search in case law for finding years)
        df['Refs_Year'] = concat_appno_year_with_search(df[['ReferTo', 'Text']])

        # Start creation of network
        print("\n\n")
        print(100 * "*")
        print("*" + 35 * " " + "Creation of Citation Network" + 35 * " " + "*")
        print(100 * "*")
        print("\nAdding nodes/case laws to the network!")
        self.add_nodes_of_network(df.AppNo_Year)

        num_of_cases_with_meta_data = len([n for n, d in self.G.nodes(data=True) if d['label'] == 'Case'])

        print("Adding citations to the network!")
        self.add_citations_to_network(df.Refs_Year)

        print(f"\n\tThere are {self.G.number_of_nodes()} nodes and {self.G.number_of_edges()} edges in the network.")
        print(f"\tThere are {num_of_cases_with_meta_data} case laws with meta-data.")

        print("\nRemove citation to duplicates.")
        self.remove_dictations_to_duplicates()

        print(f"\tThere are {self.G.number_of_nodes()} nodes and {self.G.number_of_edges()} edges in the network.")

        node_dup = duplicates_receive_citations(self.G)
        assert not node_dup, f"For the duplicate case '{node_dup}', you have {self.G.in_degree(node_dup)} incoming citations."

        # print(
        #     f"There are {len([n for n, d in self.G.nodes(data=True) if d['label'] == self.duplicate_symbol])} duplicate nodes.\n")

        assert nx.is_directed_acyclic_graph(
            self.G), 'The network is not DAG (after creating citation network using dataframe).'

        print(f"\nThere are {len([n for n in self.G.nodes() if n[-4:] == 'None'])} nodes with unknown year.\n"
              f"Concatenating two nodes with the same application number: ")
        print("\t-one with known judgment year and another with unknown judgment year!")
        self.concatenate_nodes_with_matching_years()

        print(f"\nThere are {self.G.number_of_nodes()} nodes and {self.G.number_of_edges()} edges in total.")

        print("Remove citation to duplicates.")
        self.remove_dictations_to_duplicates()

        node_dup = duplicates_receive_citations(self.G)
        assert not node_dup, f"For the duplicate case '{node_dup}', you have {self.G.in_degree(node_dup)} incoming citations."

        # or if you want to ensure your graph stays DAG use the following
        # self.merge_cases_with_DAG_check()
        # assert nx.is_directed_acyclic_graph(self.G), 'The network is not DAG (after merging nodes with no meta-data.'

        # assert nx.is_directed_acyclic_graph(
        #     self.G), 'The network is not DAG.'

        self.set_node_attributes_from_dataframe(df['AppNo_Year'], df[attrs])

        num_of_dup_nodes = len([n for n, d in self.G.nodes(data=True) if d['label'] == self.duplicate_symbol])
        num_of_case_nodes = len([n for n, d in self.G.nodes(data=True) if d['label'] == 'Case'])
        num_of_dup_edges = len(
            [n for n, m, d in self.G.edges(data=True) if d['label'] == self.duplicate_symbol.upper() + "_TO_CASE"])
        num_of_case2case_edges = len(
            [n for n, m, d in self.G.edges(data=True) if d['label'] == "CASE_TO_CASE"])
        num_of_cases_no_year = len([n for n in self.G.nodes() if n[-4:] == 'None'])

        print("\n\nSUMMARY:")
        print(f"\tThere are {self.G.number_of_nodes()} nodes and {self.G.number_of_edges()} edges in the network.")
        print("Among them:")
        print(f"\t\t- There are {num_of_case_nodes} case laws, and {num_of_dup_nodes} duplicate nodes.")
        print(f"\t\t- There are {num_of_case2case_edges} citations (CASE_TO_CASE), and {num_of_dup_edges} 'DUPLICATE_TO_CASE' edges.")

        print(f"\nThere are {num_of_cases_with_meta_data} case laws with meta-data and {num_of_cases_no_year} case laws with unknown judgment year.\n\n")

        return self.G


