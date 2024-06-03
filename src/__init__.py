# Makes the directory a Python package.


"""
EctHR Graph Database Package

This package provides tools for creating a graph database of case law citations from the European Court of Human Rights using NetworkX and Neo4j.
"""

# Importing submodules for easier access
from .data_loader import load_data
from .network_builder import create_network
# from .neo4j_loader import Neo4jLoader
# from .utils import preprocess_data

__all__ = [
    'load_data',
    'create_network',
    # 'Neo4jLoader',
    # 'preprocess_data'
]

