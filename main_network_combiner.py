
from src.args import get_combiner_args
from src.data_loader import CaseDataPreprocessor
from src.utils import save_graph_to_json, load_graph_from_json
from src.network_combiner import NetworkCombiner


args = get_combiner_args()

judgment_graph = args.judgment_graph
decision_graph = args.decision_graph
to_read_dir = args.graphs_dir
graph_name = args.graph

if args.graph_dir is None:
    to_save_dir = to_read_dir

net_combiner = NetworkCombiner(
    graph_directory=to_read_dir,
    judgment_graph=judgment_graph,
    decision_graph=decision_graph)


G = net_combiner.do_concatenation()

save_graph_to_json(G, filename=graph_name)