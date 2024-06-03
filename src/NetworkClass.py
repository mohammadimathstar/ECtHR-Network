import networkx as nx
from networkx.readwrite import json_graph

import pandas as pd
import json

import re

import collections

from utils import *


class CitationNetworks(nx.DiGraph):
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
    def __init__(self, category: str="JUD"):
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
        self.category = category

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

    def add_cases_no_citation(self, appnos_year: pd.Series, delimiter=";", node_type='Case'):
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

        # If there is an edge from the target to the source
        for (t, s, d) in self.G.in_edges(source, data=True):
            if self.match_nodes_names(t, target):
                return True

        # If this edge has already existed
        if (source, target) in self.G.out_edges(source):
            return True
        return False

    def add_case_to_case_citations(self, citations_year: pd.Series, delimiter=";", node_type='Case', edge_type="CASE_TO_CASE"):
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

    def set_node_attributes_from_dataframe(self, node_ids: pd.Series, attributes_df: pd.DataFrame):
        """
        Set node attributes from a DataFrame containing node identifiers and corresponding attributes.

        Parameters
        ----------
        node_ids : pd.Series
            Series containing node identifiers.
        attributes_df : pd.DataFrame
            DataFrame containing attributes to be set for each node.

        Raises
        ------
        AssertionError
            If the number of nodes and the number of attributes do not match.

        Returns
        -------
        None
        """
        assert len(node_ids) == len(attributes_df), f'Number of nodes {len(node_ids)} and the number of attributes {len(attributes_df)} should be the same'

        for attribute_name in attributes_df.columns:
            node_attribute_dict = {}
            for node_id in node_ids:
                if isinstance(node_id, str):
                    node_identifier = node_id.split(";")[0]
                else:
                    node_identifier = " "
                node_attribute_dict[node_identifier] = attributes_df.loc[node_id, attribute_name]

            nx.set_node_attributes(self.G, node_attribute_dict, name=attribute_name)

    def df2network(self, df, attrs: list):
        df.dropna(subset=['AppNo'], inplace=True)
        df.set_index('AppNo', inplace=True, drop=False)

        # adding the name of the dataset (HUDOC) as an attribute
        df['data_name'] = self.dataname
        attrs.append("data_name")

        df['AppNo_Year'] = concat_appno_year(df.JudgmentDate) #[['appno', 'kpdate']])
        df = df[~df.duplicated(subset=['AppNo_Year'])]
        df.set_index('AppNo_Year', inplace=True, drop=False)
        df['Refs_Year'] = concat_appno_year_with_search(df[['ReferTo', 'Text']])

        print("Creating cases' nodes")
        self.add_cases_no_citation(df.AppNo_Year)
        self.add_case_to_case_citations(df.Refs_Year)

        assert nx.is_directed_acyclic_graph(self.G), 'The network is not DAG (after creating citation network using dataframe).'

        print(f"there are {len([n for n in self.G.nodes() if n[-4:]=='None'])} with unknown year.")
        merge_cases_with_matching_years(self.G)
        # or if you want to ensure your graph stays DAG use the following
        # self.merge_cases_with_DAG_check()
        # assert nx.is_directed_acyclic_graph(self.G), 'The network is not DAG (after merging nodes with no meta-data.'

        print(f"\nThere are {len(self.G.nodes())} nodes in total.")
        print(f"There are {len(self.G.edges())} edges in total.")

        print(f"there are {len([n for n in self.G.nodes() if n[-4:]=='None'])} with unknown year.")
        print(f"there are {len([n for n, d in self.G.nodes(data=True) if d['label']==self.duplicate_symbol])}")

        # self.set_attr_of_cases(df['AppNo_Year'], df[attrs]) # instead of this line, we use the following line
        self.set_node_attributes_from_dataframe(df['AppNo_Year'], df[attrs])

        num_of_dup_nodes = len([n for n, d in self.G.nodes(data=True) if d['label']==self.duplicate_symbol])
        num_of_dup_edges = len([n for n, m, d in self.G.edges(data=True) if d['label']==self.duplicate_symbol.upper()+"_TO_CASE"])
        # assert num_of_dup_nodes==num_of_dup_edges, "there are %i of duplicated nodes, but %i of DUPLICATE_TO_CASE" % (num_of_dup_nodes, num_of_dup_edges)
        print("Note: there are %i of duplicated nodes, but %i of DUPLICATE_TO_CASE" % (num_of_dup_nodes, num_of_dup_edges))

        return df

def merge_nodes():
    # load two graphs
    G_jud = load_graph_from_json(fname='data/graphJUD')
    G_dec = load_graph_from_json(fname='data/graphDEC')

    G = nx.DiGraph()

    num_dec2jud = 0
    num_jud2dec = 0
    num_rem = 0
    num_jud_and_dec = 0
    num_jud_no_change = 0
    num_dec_no_change = 0
    for node1, data1 in G_jud.nodes(data=True):
        # print(len(data1), end=" ")
        if node1 in G_dec.nodes():
            data2 = G_dec.nodes[node1]
            if len(data1)==1 and len(data2)==1:
                # if dupl then make it dupl, otherwise case
                if data1['label']=='Duplicate' and data2['label']!='Duplicate':
                    # use data1
                    G.add_node(node1, **data1)
                    num_dec2jud = num_dec2jud+1
                elif data1['label']!='Duplicate' and data2['label']=='Duplicate':
                    # use data2
                    G.add_node(node1, **data2)
                    num_jud2dec = num_jud2dec+1
                elif data1['label']!='Duplicate' and data2['label']!='Duplicate':
                    # remove one of them
                    G.add_node(node1, **data1)
                    num_rem = num_rem+1
                else:
                    # both are duplicate
                    G.add_node(node1 + "_JUD", **data1)
                    G.add_node(node1 + "_DEC", **data2)
                    num_jud_and_dec = num_jud_and_dec+1

            elif len(data1)==1 and len(data2)>1:
                # use data2
                G.add_node(node1, **data2)
                num_jud2dec = num_jud2dec+1
            elif len(data1)>1 and len(data2)==1:
                # use data1
                G.add_node(node1, **data1)
                num_dec2jud = num_dec2jud+1
            else:
                # one node for dec, and one node for judgement
                G.add_node(node1 + "_JUD", **data1)
                G.add_node(node1 + "_DEC", **data2)
                num_jud_and_dec = num_jud_and_dec+1
                print("This case (%s) is present for both decisions and judgments with meta-data" % node1)

        else:
            G.add_node(node1, **data1)
            num_jud_no_change = num_jud_no_change+1



    for node2, data2 in G_dec.nodes(data=True):
        if node2 not in G_jud.nodes():
            G.add_node(node2, **data2)
            num_dec_no_change = num_dec_no_change+1

    assert len(G.nodes())==(num_jud_no_change + num_dec_no_change + num_dec2jud + num_jud2dec + num_rem + 2*num_jud_and_dec)

    print(f"{num_dec2jud} of decision was merged into judgements")
    print(f"{num_jud2dec} of judgements was merged into decisions")
    print(f"{num_rem} of decision was removed (there is a judgment with the same appno. and they are both duplicate)")
    print(f"{num_jud_and_dec} cases of decisions and judgements have the same app. no. (without features)")

    with_meta_data = [p for p, d in G.nodes(data=True) if len(d) > 1]
    print("\nThere are %i cases with meta-data" % len(with_meta_data))
    print("There are %i cases without meta-data" % (len(G.nodes()) - len(with_meta_data)))


    # NEW: remove nodes without year
    # l = [n for n in G.nodes() if "None" in n]
    # G.remove_nodes_from(l)
    # print("\nThere are %i cases without year, and we delete them from the graph." % len(l))


    save_graph_to_json(G, fname='graph_JUD_DEC')

    return G

def merge_edges(G):
    # load two graphs
    G_jud = load_graph_from_json(fname='data/graphJUD')
    G_dec = load_graph_from_json(fname='data/graphDEC')

    for node1 in G.nodes():
        if node1[-4:]=="_JUD":
            # related to judgements
            for (_, n, d) in G_jud.out_edges(node1[:-4], data=True):
                if n in G.nodes():
                    G.add_edge(node1, n, **d)
                else:
                    assert n+"_JUD" in G.nodes(), "the node %s (and %s_JUD) is not in the graph" % (n, n)
                    G.add_edge(node1, n+"_JUD", **d)

        elif node1[-4:]=="_DEC":
            # related to decisions
            for (_, n, d) in G_dec.out_edges(node1[:-4], data=True):
                if n in G.nodes():
                    G.add_edge(node1, n, **d)
                else:
                    assert n+"_DEC" in G.nodes(), "the node %s (and %s_DEC) is not in the graph" % (n, n)
                    G.add_edge(node1, n+"_DEC", **d)

        else:
            # it is a unique appno (either judgements or decisions)
            if (node1 in G_jud.nodes()) and (node1 not in G_dec.nodes()) :
                for (_, n, d) in G_jud.out_edges(node1, data=True):
                    if n in G.nodes():
                        G.add_edge(node1, n, **d)
                    else:
                        assert n+"_JUD" in G.nodes(), "the node %s (and %s_JUD) is not in the graph" % (n, n)
                        G.add_edge(node1, n+"_JUD", **d)
            elif (node1 in G_dec.nodes()) and (node1 not in G_jud.nodes()):
                # assert node1 in G_dec.nodes(), "node %s does belong neither judgements nor decisions" % node1
                for (_, n, d) in G_dec.out_edges(node1, data=True):
                    if n in G.nodes():
                        G.add_edge(node1, n, **d)
                    else:
                        assert n+"_DEC" in G.nodes(), "the node %s (and %s_DEC) is not in the graph" % (n, n)
                        G.add_edge(node1, n+"_DEC", **d)
            elif (node1 in G_jud.nodes()) and (node1 in G_dec.nodes()) : #node is present on both
                # only one of them is duplicate or none of them is duplicate
                for (_, n, d) in G_jud.out_edges(node1, data=True):
                    if n in G.nodes():
                        G.add_edge(node1, n, **d)
                    else:
                        assert n+"_JUD" in G.nodes(), "the node %s (and %s_JUD) is not in the graph" % (n, n)
                        G.add_edge(node1, n+"_JUD", **d)
                for (_, n, d) in G_dec.out_edges(node1, data=True):
                    if n in G.nodes():
                        G.add_edge(node1, n, **d)
                    else:
                        assert n+"_DEC" in G.nodes(), "the node %s (and %s_DEC) is not in the graph" % (n, n)
                        G.add_edge(node1, n+"_DEC", **d)
            else:
                assert True, "node %s is not found both in judgements and in decisions" % node1
            if (node1 in G_jud.nodes()) and (node1 in G_dec.nodes()):
                if (len(G_jud.nodes[node1])!=1) and (len(G_dec.nodes[node1])>1):
                    assert True, "node %s is found both in judgements and in decisions (but they saved without _JUD and _DEC)" % node1

    return G

def check_duplicate(G):

    print(f"there are {len(G.nodes())} nodes and {len(G.edges())} edges (BEFORE)")
    for node, data in G.nodes(data=True):
        if data['label']=='Duplicate':
            out_neighbors = [n for (_, n) in G.out_edges(node)]
            assert len(out_neighbors)==1, "node %s (a duplicate case) has %i outgoing citations." % (node, len(out_neighbors))
            pointAt = out_neighbors[0]
            assert G.nodes[pointAt]['label']=='Case', "node %s (a duplicate case) point to %s (which is not case but a %s)" % (node, pointAt, G.nodes[pointAt]['label'])

            l = [(node2, data2) for (node2, _, data2) in G.in_edges(node, data=True)]# if (node2, pointAt) not in G.edges()]
            for (n, d) in l:
                if (n, pointAt) not in G.edges():
                    G.add_edge(n, pointAt, **d)
                G.remove_edge(n, node)
            assert len(G.in_edges(node))==0, 'there are still some incoming citation to the duplicated case %s' % node


    print(f"there are {len(G.nodes())} nodes and {len(G.edges())} edges (AFTER)")
    return G



class preprocess:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy(deep=True)
        self.rename = {"appno": "AppNo", 'docname': 'Title', 'doctype': 'DocType',
                'importance': 'ImportanceLevel', 'extractedappno': 'ReferTo',
                'originatingbody': 'OriginatingBody', 'doctypebranch': 'Chamber',
                'respondent': 'Respondent', 'conclusion': 'Conclusion',
                'article': "Article", 'kpdate': "JudgmentDate", 'text': 'Text'
                # 'typedescription': 'TypeDescription',
                }


    def drop_cols(self):
        self.df = self.df[self.rename.keys()]

    def change_cols_name(self):
        self.df.rename(columns=self.rename, inplace=True)

    def change_categorical_var(self):
        dic_imp = {1: 'Key Case', 2: 1, 3: 2, 4: 3}
        dic_org_body = {4: 'Court (first section)', 5: 'Court (second section)', 6: 'Court (third section)', 7: 'Court (fourth section)', 8: 'Court (grand chamber)', 9: 'Court (chamber)', 15: 'Court (plenary)', 23: 'Court (fifth section)', 25: 'Court (first section committee)', 26: "Court (second section committee)", 27: "Court (third section committee)", 28: "Court (fourth section committee)", 29: "Court (fifth section committee)"
                        }
        dic_doc_type = {'HEJUD': "JUDGMENT", "HEDEC": "DECISION", "HEJP9": "JP9", "HECOM": "COMMUNICATED_CASE"}
        self.df.replace({
            "ImportanceLevel": dic_imp,
            "OriginatingBody": dic_org_body,
            "DocType": dic_doc_type,
                         },
                        inplace=True
                        )

    def check_articles(self, s):
        return self.df.loc[self.df.Article.str.contains(s, na=False, regex=False), ['AppNo', 'Article']]


    def extract_articles(self):
        l = []
        for _, articles in self.df.Article.dropna().iteritems():
            a = articles.split(";")
            l.extend(a)

        c = collections.Counter(l)
        print(f"There are {len(c)} possible values.")
        print(c.keys())

        return c

    def extract_invalid_appno(self):
        l = []
        print(self.df.columns)
        for _, (app, refs) in self.df.dropna(subset='ReferTo')[['AppNo', 'ReferTo']].iterrows():

            s = re.findall(r"(\d?\d?\d\d\d\d/\d\d)", refs)
            if len(refs.split(";"))!=len(s):
                l.append((app, refs))

            # citations = refs.split(";")
            # for citation in citations:
            #     if

            # break
        return l

    def remove_self_ref(self):
        for i, (appno, refs) in self.df[['AppNo', 'ReferTo']].iterrows():
            if isinstance(appno, str) and isinstance(refs, str) and refs != "":
                for s in appno.split(';'):
                    self.df.loc[i, 'ReferTo'] = self.df.loc[i, 'ReferTo'].replace(s, "")
                    l = [p for p in self.df.loc[i, 'ReferTo'].split(';') if p != ""]
                    self.df.loc[i, 'ReferTo'] = ";".join(l)

    def do_preprocess(self):
        self.drop_cols()
        self.change_cols_name()
        self.change_categorical_var()
        self.remove_self_ref()


