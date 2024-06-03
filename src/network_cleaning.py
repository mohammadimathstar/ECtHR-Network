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


def merge_nodes():
    # load two graphs
    G_jud = load_graph_from_json(fname='graphJUD')
    G_dec = load_graph_from_json(fname='graphDEC')

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
    print(f"{num_rem} of decision was removed")
    print(f"{num_jud_and_dec} cases of decisions and judgements have the same app. no. (without features)")

    with_meta_data = [p for p, d in G.nodes(data=True) if len(d)>1]
    print("\nThere are %i cases with meta-data" % len(with_meta_data))
    print("There are %i cases without meta-data" % (len(G.nodes()) - len(with_meta_data)))


    # NEW: remove nodes without year
    l = [n for n in G.nodes() if "None" in n]
    G.remove_nodes_from(l)
    print("\nThere are %i cases without year, and we delete them from the graph." % len(l))


    save_graph_to_json(G, fname='graph_JUD_DEC')

    return G

def merge_edges(G):
    # load two graphs
    G_jud = load_graph_from_json(fname='graphJUD')
    G_dec = load_graph_from_json(fname='graphDEC')

    for node1 in G.nodes():
        if node1[-4:] == "_JUD":
            # related to judgements
            for (_, n, d) in G_jud.out_edges(node1[:-4], data=True):
                if n in G.nodes():
                    G.add_edge(node1, n, **d)
                else:
                    assert n+"_JUD" in G.nodes(), "the node %s (and %s_JUD) is not in the graph" % (n, n)
                    G.add_edge(node1, n+"_JUD", **d)

        elif node1[-4:] == "_DEC":
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


def get_graph_without_duplicate():
    G = load_graph_from_json('./data/graph_JUD_DEC')
    print(f"There are {len(G.nodes())} nodes and {len(G.edges())} edges (BEFORE removal of DUPLICATEs).")

    # remove duplicate nodes
    duplicate_list = []
    for n, d in G.nodes(data=True):
        if d['label'] == 'Duplicate':
            duplicate_list.append(n)
    G.remove_nodes_from(duplicate_list)

    print(f"There are {len(G.nodes())} nodes and {len(G.edges())} edges (AFTER removal of DUPLICATEs). \n")

    return G


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