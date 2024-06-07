
import os
import argparse
import pickle


def get_builder_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser('Create The Citation Network of European Court of Human Rights.')
    parser.add_argument('--doctype',
                        type=str,
                        default='JUDGMENTS',
                        help='The type of legal document: a) JUDGMENTS, b) DECISIONS.')
    parser.add_argument('--graph_name',
                        type=str,
                        default='graph',
                        help='The name to save the graph.')
    parser.add_argument('--csv_dir',
                        type=str,
                        default='./data/download',
                        help='The directory to read meta-data (which is saved as .csv file).')
    parser.add_argument('--graph_dir',
                        type=str,
                        default='./data',
                        help='The directory to save the constructed citation network.')

    args = parser.parse_args()
    return args


def get_combiner_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser('Merge the Citation Networks of Judgments and Decisions.')

    parser.add_argument('--judgment_graph',
                        type=str,
                        default='graphJUD',
                        help='The name to the file containing judgments graph.')
    parser.add_argument('--decision_graph',
                        type=str,
                        default='graphDEC',
                        help='The name to the file containing decision graph.')
    parser.add_argument('--graph',
                        type=str,
                        default='graph_JUDDEC',
                        help='The name to save the constructed graph.')
    parser.add_argument('--graphs_dir',
                        type=str,
                        default='./data',
                        help='The directory to read networks.')
    parser.add_argument('--graph_dir',
                        type=str,
                        default=None,
                        help='The directory to save the constructed citation network.')

    args = parser.parse_args()
    return args


def get_neo4j_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser('Load the Citation Network to Neo4j.')

    parser.add_argument('--graph',
                        type=str,
                        default='graph_JUDDEC',
                        help='The name to save the constructed graph.')
    parser.add_argument('--graphs_dir',
                        type=str,
                        default='./data',
                        help='The directory to read networks.')
    parser.add_argument('--uri',
                        type=str,
                        default='bolt://localhost:7687',
                        help='The URI of the Neo4j database.')
    parser.add_argument('--username',
                        type=str,
                        default='neo4j',
                        help='The username for Neo4j authentication.')
    parser.add_argument('--password',
                        type=str,
                        default='12345678',
                        help='The password for Neo4j authentication.')

    args = parser.parse_args()
    return args

