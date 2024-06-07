
from src.utils import load_graph_from_json
from src.args import get_neo4j_args
from src.neo4j_loader import Neo4jLoader

import os


args = get_neo4j_args()

uri = args.uri
auth = (args.username, args.password)

graph_name = args.graph
graph_dir = args.graphs_dir

graph_address = os.path.join(graph_dir, graph_name)
graph = load_graph_from_json(graph_address)

neo4jloader = Neo4jLoader(uri, auth)

neo4jloader.create_graph(graph)

neo4jloader.close()
