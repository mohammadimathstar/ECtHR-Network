import networkx as nx
from typing import List
import json
import pandas as pd
import numpy as np


from utils import load_graph_from_json, save_graph_to_json, load_data



def filtering_network_by_article(G: nx.DiGraph, articles: List[str]):
    if not isinstance(articles, list):
        articles = [articles]

    nodes = []

    for article in articles:
        # extracting decisions/judgments which are related to an article
        df_jud = load_data(col='JUDGMENTS', article=article)
        df_dec = load_data(col='DECISIONS', article=article)

        appnos = df_jud.index.to_list()
        appnos.extend(df_dec.index.to_list())

        doctypes = ["JUD"] * len(df_jud) + ["DEC"] * len(df_dec)

        del df_jud
        del df_dec
        for (appno, doctype) in zip(appnos, doctypes):
            if (appno.split(";")[0] in G.nodes()):
                nodes.append(appno.split(";")[0])
            elif appno.split(";")[0] + "_" + doctype[:3] in G.nodes():
                nodes.append(appno.split(";")[0] + "_" + doctype[:3])
            else:
                print(f"the node {appno} of type {doctype} is not in the graph")

    nodes_incoming_citations = set([m for n in nodes for (m, _) in G.in_edges(n) if m not in nodes])
    nodes_outgoing_citations = set()# set([m for n in nodes for (_, m) in G.out_edges(n) if m not in nodes])

    nodes_ext = nodes + list(nodes_incoming_citations.union(nodes_outgoing_citations))

    H = G.subgraph(nodes)
    H_ext = G.subgraph(nodes_ext)

    print(f"There are {len(H.nodes())} cases and {len(H.edges())} citations, related to article {articles}.")
    print(f"There are {len(H_ext.nodes())} cases and {len(H_ext.edges())} citations, related to article {articles} (extended version).")

    return H, H_ext



def get_case2node(G: nx.DiGraph, appnoyears: List[str], doctypes: List[str]):
    nodes = []
    absent_cases = []
    for case, doctype in zip(appnoyears, doctypes):
        if (case.split(";")[0] in G.nodes()):
            nodes.append(case.split(";")[0])
        elif (case.split(";")[0] + "_" + doctype[:3] in G.nodes()):
            nodes.append(case.split(";")[0] + "_" + doctype[:3])
        else:
            absent_cases.append((case, doctype))

    print(f"{len(absent_cases)} cases are not in the network, including: {absent_cases}")
    return nodes



def correct_duplicate_to_case_edges(G):
    """
    sometime an application number is for more than one cases (once as a duplicate, and another time as one case)
    :return:
    """
    num_of_changes = 0
    num_of_remove = 0
    for (n, m, d) in G.edges(data=True):
        if d['label']=='DUPLICATE_TO_CASE' and G.nodes[n].get('JudgmentDate', None)!=None:
            year_n = n.split(":")[1][:4]
            year_m = m.split(":")[1][:4]
            if int(year_m)>int(year_n):
                G.remove_edge(n, m)
                num_of_remove += 1
            else:
                attrs = {(n, m): {'label': 'CASE_TO_CASE', 'citedAt': year_n}}
                nx.set_edge_attributes(G, attrs)
                num_of_changes += 1

    print(f"{num_of_remove} of edges were removed, and {num_of_changes} of 'DUPLICATE_TO_CASE' edges were changed to 'CASE_TO_CASE'.")
    return G


def remove_case_without_year(G: nx.DiGraph):
    print(f"There are {len(G.nodes())} nodes and {len(G.edges())} edges (BEFORE removal of cases with no-year).")

    cases_without_year = []
    for n in G.nodes():
        if "None" in n:
            cases_without_year.append(n)
    G.remove_nodes_from(cases_without_year)
    print(f"There are {len(cases_without_year)} cases with no year.")
    print(f"There are {len(G.nodes())} nodes and {len(G.edges())} edges (AFTER removal of cases with no-year). \n")

    return G

def merging_missing_meta_data(G):
    node2appno_with_meta = {n: n.split(":")[0] for n, d in G.nodes(data=True) if
                            d.get('JudgmentDate', None) != None and d['label'] != 'Duplicate'}
    # node2appno_with_meta = {n: n[:-5] for n, d in G.nodes(data=True) if
                            # d.get('JudgmentDate', None) != None and d['label'] != 'Duplicate'}
    appno2node_with_meta = {v: k for k, v in node2appno_with_meta.items()}
    node2appno_without_meta = {n: n.split(":")[0] for n, d in G.nodes(data=True) if
                               d.get('JudgmentDate', None) is None and d['label'] != 'Duplicate'}
    # node2appno_without_meta = {n: n[:-5] for n, d in G.nodes(data=True) if
    #                            d.get('JudgmentDate', None) is None and d['label'] != 'Duplicate'}
    # appno2node_without_meta = {v: k for k, v in node2appno_without_meta.items()}

    print("There are %i nodes with meta data." % len(node2appno_with_meta))
    print("There are %i nodes without meta data." % len(node2appno_without_meta))

    d = dict()
    for node, appno in node2appno_without_meta.items():
        if appno in appno2node_with_meta.keys():
            incoming_citation = [(m, data) for (m, _, data) in G.in_edges(node, data=True)]

            if len(incoming_citation) == 1:
                source = incoming_citation[0][0]
                target = appno2node_with_meta[appno]
                data = incoming_citation[0][1]
                d[node] = target
                G.add_edge(source, appno2node_with_meta[appno], **data)

    G.remove_nodes_from(d.keys())

    print(f"\nThere are {len(d)} cases with wrong year (without any meta-data).")
    print("After merging them with nodes with the correct year, we have %i nodes.\n" % len(G.nodes()))

    return G


def remove_nodes_without_meta_data_and_one_neighbor(G):
    # since nodes (without meta-data) with only one incoming citations is not important, we remove them

    without_meta = [n for n, d in G.nodes(data=True) if d.get('JudgmentDate', None) is None]
    with_one_neighbor = []
    for n in without_meta:
        nei = [m for (m, _) in G.in_edges(n)]
        if len(nei) == 1:
            with_one_neighbor.append(n)

    print("There are %i nodes without any meta-data and only one incoming citation." % len(with_one_neighbor))

    G.remove_nodes_from(with_one_neighbor)
    print("After removing them, we have %i nodes.\n" % len(G.nodes()))

    print("Now, there are only %i nodes without any meta-data." % (len(without_meta) - len(with_one_neighbor)))

    return G

def remove_cycles_of_length_two(G):
    """
    for some cases, we see that two cases cites each other. In order to solve this issue, we remove one of the edges.
    Example: see 8805/79:1984 and 9626/81:1984 (the date for both is 22 May 1984)
    :return:
    """
    from datetime import datetime

    edges2remove = []
    for (n, m, d) in G.edges(data=True):
        # source_year, target_year = int(n.split(":")[1][:4]), int(m.split(":")[1][:4])
        if (m, n) in G.edges() and (m, n) not in edges2remove:
            s_date_n = G.nodes[n].get('JudgmentDate', None)
            s_date_m = G.nodes[m].get('JudgmentDate', None)
            if s_date_m!=None and s_date_n!=None:
                date_n = datetime.strptime(s_date_n, '%Y-%m-%d').date()
                date_m = datetime.strptime(s_date_m, '%Y-%m-%d').date()
                if date_m==date_n:
                    edges2remove.append((n, m))
                elif date_m>date_n:
                    edges2remove.append((n, m))

    G.remove_edges_from(edges2remove)

    print(f"{len(edges2remove)} of edges were removed because there were cycle of length 2.")

    return G

def remove_reversal_edges(G):
    edges2remove = []
    for (n, m, d) in G.edges(data=True):
        source_year, target_year = int(n.split(":")[1][:4]), int(m.split(":")[1][:4])
        assert int(d['citedAt']) == source_year, 'there is an inconsistency between source year and citation year.'

        if source_year<target_year:
            edges2remove.append((n, m))

    G.remove_edges_from(edges2remove)
    print(f"{len(edges2remove)} of edges were removed because the date of source was before of the date of target.")

    return G

def filtering_nodes_without_meta_data(G: nx.DiGraph):
    # remove nodes without meta-data and with only one-incoming citation
    G = merging_missing_meta_data(G)
    G = remove_nodes_without_meta_data_and_one_neighbor(G)

    return G

def do_transitive_reduction(G):
    import copy
    if nx.is_directed_acyclic_graph(G):
        return nx.transitive_reduction(G)

    # if it is not DAG (i.e. there are cycles in the graph): in order to remove cycles, we remove one edge from each cycle
    first_edge_of_cycles = []
    for l in nx.simple_cycles(G):
        first_edge = (l[0], l[1])
        first_edge_of_cycles.append(first_edge)

    G_DAG = nx.DiGraph(copy.deepcopy(G))
    G_DAG.remove_edges_from(first_edge_of_cycles)
    assert nx.is_directed_acyclic_graph(G_DAG), f"There are still {len(list(nx.simple_cycles(G_DAG)))} cycles in H_DAG, like {nx.find_cycle(G_DAG, orientation='original')}."
    print(f"We removed {len(G.edges()) - len(G_DAG.edges())} citations in order to remove cycles.")

    G_DAG_trans_reduction = nx.transitive_reduction(G_DAG)
    # print(f"There are {len(H_DAG_trans_reduction.nodes())} cases and {len(H_DAG_trans_reduction.edges())} citations.")
    print(f"During transitive reduction, we removed {2 * len(G_DAG.edges())-len(G_DAG_trans_reduction.edges()) - len(G_DAG.edges())} citations (in total: {len(G.edges()) - len(G_DAG_trans_reduction.edges())}).")
    return G_DAG_trans_reduction