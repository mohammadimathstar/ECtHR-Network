# Creation of Neo4j Graph Database for ECtHR Citation Network

This repository contains a Python script that converts a network created using Networkx into a Graph Database in Neo4j using its Python API.

## Requirements

To run the script, you need the following dependencies:

- Python 3.x
- Networkx
- Neo4j
- Neo4j Python driver (neo4j-driver)

## Installation

1. Install Python 3.x from [python.org](https://www.python.org/).
2. Install Networkx using pip:

```bash
pip install networkx
```
3. Install Neo4j on your system. You can download it from neo4j.com.
4. Install the Neo4j Python driver using pip:
```bash
pip install neo4j
```

## Overview
To create the Graph Database, we need to perform the following steps:
1) Download meta-data and texts from HUDOC.
2) Create citation networks (using Networkx) of judgments and decisions, separately.
3) Merge two citation networks (judgments and decisions).
4) Create Neo4j Graph Database.

## 1) Download HUDOC
To download HUDOC database, use the code presented in [HUDOC](https://github.com/WillSkywalker/HUDOCcrawler).

After downloading the HUDOC database (containing both meta-data and texts), we create a dataframe containing:
- columns containing of meta-data, and
- `'text'` column: containing the text of case laws.

The dataframe is saved as .csv file, called `JUDGMENTS_full.csv` and `DECISIONS_full.csv`, in the `./data/download/` director.

## 2) Creation of the citation network (Networkx)
To represent a case law in the network, we use this pattern:
- `'application_number:year'`: its application number + the year of the judgment

  
To create the citation network for judgments:

```bash
python main_network_builder.py --doctype JUDGMENTS --graph_name graphJUD --csv_dir './data/download/' --graph_dir './data'
```

To create the citation network for decisions:

```bash
python main_network_builder.py --doctype DECISIONS --graph_name graphDEC --csv_dir './data/download/' --graph_dir './data'
```

## 3) Combining Citation Networks (Judgments and Decisions)
To merge the judgment citation network and the decision citation network:
```bash
python main_network_combiner.py --judgment_graph 'graphJUD' --decision_graph 'graphDEC' --graph 'graph_JUDDEC' --graphs_dir './data'
```


## 4) Create Neo4j Graph Database
To convert a network of type `Networkx` to a Neo4j Graph Database:
```bash
python main_neo4j_loader.py --graph 'graph_JUDDEC' --graphs_dir './data' --uri 'bolt://localhost:7687' --username ... --password ... 
```
