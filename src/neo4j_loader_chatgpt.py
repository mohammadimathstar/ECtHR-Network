# Code to create the database in Neo4j.

from neo4j import GraphDatabase

class Neo4jLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()

    def create_graph(self, graph):
        with self.driver.session() as session:
            for node in graph.nodes:
                session.write_transaction(self._create_node, node)
            for edge in graph.edges:
                session.write_transaction(self._create_edge, edge)
    
    @staticmethod
    def _create_node(tx, node):
        tx.run("CREATE (n:Case {name: $name})", name=node)

    @staticmethod
    def _create_edge(tx, edge):
        tx.run("MATCH (a:Case {name: $source}), (b:Case {name: $target}) "
               "CREATE (a)-[:CITES]->(b)", source=edge[0], target=edge[1])

