import os
import json
import networkx as nx
import pandas as pd
from networkx.readwrite import json_graph
from typing import Literal, Union, List, Tuple
import re
from itertools import chain


def save_graph_to_json(graph: nx.DiGraph, filename: str='graph.json') -> None:
    """
    Save a NetworkX directed graph to a JSON file using node-link data format.

    Parameters:
    graph (nx.DiGraph): The directed graph to be saved.
    filename (str): The name of the output JSON file. Defaults to 'graph.json'.
    """
    if not filename.endswith(".json"):
        filename += ".json"
    
    data = json_graph.node_link_data(graph)
    
    # Ensure the "data" directory exists
    directory = "./data"
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    filepath = os.path.join(directory, filename)
    with open(filepath, "w") as file:
        json.dump(data, file)

def load_graph_from_json(file_path: str='graph.json') -> nx.DiGraph:
    """
    Load a NetworkX directed graph from a JSON file using node-link data format.

    Parameters:
    filename (str): The name of the input JSON file. Defaults to 'graph.json'.

    Returns:
    nx.DiGraph: The loaded directed graph.
    """
    if not file_path.endswith(".json"):
        file_path += ".json"
    
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    graph = json_graph.node_link_graph(data)
    return graph


def filter_articles(df: pd.DataFrame, article: str = '8') -> pd.Series:
    """
    Filter a DataFrame to identify rows where the specified article is present.

    Parameters:
    df (pd.DataFrame): The input DataFrame containing an 'article' column.
    article (str): The article number to filter by. Defaults to '8'.

    Returns:
    pd.Series: A boolean Series indicating which rows contain the specified article.
    """
    def contains_article(article_list, article):
        if isinstance(article_list, str):
            return article in article_list.split(";")
        return False

    # Apply the contains_article function to the 'article' column
    mask = df['article'].apply(contains_article, article=article)
    return mask



def load_data(doc_type: Literal['JUDGMENTS', 'DECISIONS'],
              article: str = '8',
              lang: List[Literal['ENG', 'FRE']] = ['ENG']) -> pd.DataFrame:
    """
    Load and filter a dataset of legal documents based on document type, article, and language.

    Parameters:
    doc_type (Literal['JUDGMENTS', 'DECISIONS']): The type of documents to load.
    article (str): The article number to filter by. Defaults to '8'.
    lang (List[Literal['ENG', 'FRE']]): The languages to filter by. Defaults to ['ENG'].

    Returns:
    pd.DataFrame: The filtered DataFrame containing only the relevant documents.
    """
    # Ensure lang is a list
    if not isinstance(lang, list):
        lang = [lang]

    # Load the dataset
    file_path = f"../data/download/{doc_type}_full.csv"
    try:
        df = pd.read_csv(file_path, index_col='appnoyear',
                         usecols=['appnoyear', 'text', 'article', 'language'])
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    except Exception as e:
        raise Exception(f"An error occurred while reading the file: {e}")

    # Filter the DataFrame by article and language
    df['filter_article'] = filter_articles(df, article=article)
    filtered_df = df[(df['filter_article']) & (df['language'].isin(lang))]

    # Drop the unnecessary columns
    filtered_df.drop(['article', 'language', 'filter_article'], axis=1, inplace=True)

    return filtered_df


def matching_nodes_names(node1: str, node2: str, citation_year: int = 3000) -> int:
    """
    Determines the level of matching between two nodes' names.
    Note that the name of nodes are in the format application_num:judgment_year.

    Parameters
    ----------
    node1 : str
        First node name in the format application_num:judgment_year.
    node2 : str
        Second node name in the format application_num:judgment_year.
    citation_year : int, optional
        Citation year (default is 3000).

    Returns
    -------
    int
        2 if fully matched, 1 if partially matched, else 0.
    """
    if node1 == node2:
        # Nodes are exactly the same
        return 2
    elif (node1[:-4] == node2[:-4]) and ('None' in [node1[-4:], node2[-4:]]):
        # Nodes have the same application number and exactly one has no judgment year
        year = node2[-4:] if node1[-4:] == 'None' else node1[-4:]
        if int(citation_year) >= int(year):
            return 1
    return 0

def does_one_of_them_exist(G, nodes: Union[List[str], str]) -> Tuple[bool, Union[str, None]]:
    """
    Checks if at least one node from the given list exists in the graph.

    Parameters
    ----------
    nodes : Union[List[str], str]
        List of node IDs or a single node ID.

    Returns
    -------
    Tuple[bool, Union[str, None]]
        (True, node) if one of the nodes exists, else (False, None).
    """
    if not isinstance(nodes, list):
        nodes = [nodes]
    for node in nodes:
        if node in G.nodes:
            return True, node
    return False, None


def find_year_in_text(s: str, appno: str, citation_year: str, window_size=100) -> str:
    """
    Extracts the judgment year of a cited case from the text of case (which cites it).

    Parameters
    ----------
    s : str
        text of the case law.
    appno : str
        Application number of the cited case law.
    citation_year : str
        Citation year.

    Returns
    -------
    str
        Extracted date or 'None'.
    """
    if not isinstance(s, str):
        s = str(s)

    l = s.find(appno)
    matches = re.findall(r"\D(\d\d\d\d)\D", s[l:l + window_size])
    val_matches = [m for m in matches if (1950 < int(m)) and (int(m) <= 2022) and (int(m) <= int(citation_year))]

    return val_matches[0] if val_matches else str(None)

def concat_appno_year(date: pd.Series, delimiter: str = ";") -> pd.Series:
    """
    concatenate application numbers and years into a single string with a delimiter.

    Parameters
    ----------
    date : pd.Series
        Series with application numbers and dates.
    delimiter : str, optional
        Delimiter to separate application numbers (default is ";").

    Returns
    -------
    pd.Series
        Series with merged application numbers and years.
    """
    # print()
    merged = [";".join([f"{n}:{data_info[:4]}" for n in appno.split(delimiter)]) for appno, data_info in
              date.items()]
    return pd.Series(merged, index=date.index, copy=False)


def concat_appno_year_with_search(df: pd.DataFrame, delimiter: str = ";", window_size: int = 100):
    """
    Concatenate application numbers and years with a search in the text.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with application numbers and text.
    delimiter : str, optional
        Delimiter to separate application numbers (default is ";").
    window_size: int, optional
        The region to search for the judgment year in the text.

    Returns
    -------
    pd.Series
        Series with merged application numbers and years after searching the text.
    """
    l = []
    for appno_year, (citations, text) in df.dropna(subset=['ReferTo']).iterrows():
        nodes = citations.split(delimiter)

        citedAt = appno_year.split(delimiter)[0][-4:]
        nodes_with_years = [f"{n}:{find_year_in_text(text, n, citedAt, window_size)}" for n in nodes]

        l.append(";".join(nodes_with_years))

    return pd.Series(l, copy=False, index=df.dropna(subset=['ReferTo']).index)


def duplicates_receive_citations(G: nx.DiGraph, duplicate_symbol: str = 'Duplicate'):
    for (n, d) in G.nodes(data=True):
        if d['label'] == duplicate_symbol:
            if G.in_degree(n) != 0:
                return n
    return False


def get_citations_to_duplicates(G: nx.DiGraph, duplicate_symbol: str = 'Duplicate'):
    edges_to_remove = []
    for n, d in G.nodes(data=True):
        if (d['label'] == duplicate_symbol) and (G.in_degree(n) != 0):
            for (s, _) in G.in_edges(n):
                edges_to_remove.append((s, n))
    return edges_to_remove


def concatenate_dict(dict1, dict2):
    """
    Concatenate two dictionaries by adding elements from the second dictionary to the first dictionary if they do not
    already exist in the first dictionary.

    Parameters
    ----------
    dict1 : dict
        The first dictionary which will be updated with elements from the second dictionary.
    dict2 : dict
        The second dictionary whose elements will be added to the first dictionary if they are not already present.

    Returns
    -------
    dict
        The updated first dictionary containing all unique elements from both dictionaries.
    """
    for (k, v) in dict2.items():
        if k not in dict1.keys():
            dict1[k] = v

    return dict1

def redirect_edges(G, v, u):
    """
    Generate a list of new edges by redirecting all edges from node `v` to node `u`. This involves redirecting all
    incoming and outgoing edges of `v` to `u`.

    Parameters
    ----------
    v : str
        The node that will be removed.
    u : str
        The node that will remain. All edges of `v` will be redirected to this node.

    Returns
    -------
    generator
        A generator yielding the new edges to be added to the graph after redirecting them from `v` to `u`.
    """
    in_edges = ((w, u, d) for w, x, d in G.in_edges(v, data=True) if w != u)
    out_edges = ((u, w, d) for x, w, d in G.out_edges(v, data=True) if w != u)
    return chain(in_edges, out_edges)


def set_node_attributes_from_dataframe(G, node_ids: pd.Series, attributes_df: pd.DataFrame):
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

        nx.set_node_attributes(G, node_attribute_dict, name=attribute_name)


def merge_cases_with_matching_years(G: nx.DiGraph):
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
                new_edges = redirect_edges(G, node1, node2)
                G.remove_node(node1)
                G.add_edges_from(new_edges)

                assert 'label' in G.nodes[node2], f'{node2} does not have a label.'
                num_of_matches += 1
                break

    print(f"\tWe merged {num_of_matches} application numbers (out of {len(nodes_without_year)})")
