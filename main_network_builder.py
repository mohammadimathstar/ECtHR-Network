
from src.args import get_builder_args
from src.data_loader import CaseDataPreprocessor
from src.utils import save_graph_to_json
from src.network_builder import CitationNetwork

import pandas as pd


args = get_builder_args()
doctype = args.doctype
graph_name = args.graph_name

metadata = [
    'Title', 'DocType', 'AppNo',  'Conclusion', 'ImportanceLevel', 'OriginatingBody',
    'JudgmentDate', 'ReferTo', 'Chamber', 'Respondent', 'Article',
]

preprocessor = CaseDataPreprocessor(doctype=doctype)
df = preprocessor.preprocess_data()

print(f"There are {len(df)} cases. {df.Text.isna().sum()} cases do not have text.")

net = CitationNetwork()
G = net.dataframe_to_network(df, attrs=metadata)

save_graph_to_json(G, filename=graph_name)