# Code to create the database in Neo4j.

from neo4j.time import DateTime
from neo4j import GraphDatabase

from typing import Union, List

class Neo4jLoader:
    """
        A class to create a Graph Database using Neo4j. It takes a graph (from Networkx) and then saves it
        inside Neo4j.
        """

    def __init__(self, uri: str, auth: tuple):
        """
        Initialize the Neo4jLoader with the given URI and authentication tuple.

        Args:
            uri (str): The URI of the Neo4j database.
            auth (tuple): A tuple containing the username and password for Neo4j authentication.
        """
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        """
        Close the connection to the Neo4j database.
        """
        self.driver.close()

    @classmethod
    def clean_db(cls, tx):
        """
        Clean the database by deleting all nodes and relationships.

        Args:
            tx: The transaction context.
        """
        query = "MATCH (n) DETACH DELETE n"
        tx.run(query)

    @classmethod
    def node_exists(cls, tx, node_id: str, labels: Union[str, List[str]]) -> bool:
        """
        Check whether a node with the given ID and labels exists.

        Args:
            tx: The transaction context.
            node_id (str): The ID of the node.
            labels (Union[str, List[str]]): The label(s) of the node.

        Returns:
            bool: True if the node exists, False otherwise.
        """
        if isinstance(labels, list):
            labels = ":".join(labels)
        query = f"OPTIONAL MATCH (n:{labels} {{id:$node_id}}) RETURN n IS NOT NULL AS node_exists"

        result = tx.run(query, node_id=node_id)
        return result.single()["node_exists"]

    @classmethod
    # def node_exists(cls, tx, id: str, label: Union[str, list]):
    #     """ to check whether a case exists or not"""
    #     if isinstance(label, list):
    #         label = ":".join(label)
    #     query = (
    #             "OPTIONAL MATCH (n:" + label + " {id:$id})"
    #                                            "RETURN n IS NOT NULL"
    #     )
    #     res = tx.run(query, id=id)
    #     return res.single()[0]

    @classmethod
    def create_new_node(cls, tx, node_id: str, attributes: dict, labels: Union[str, List[str]]):
        """
        Create a new node with the given ID, attributes, and labels.

        Args:
            tx: The transaction context.
            node_id (str): The ID of the node.
            attributes (dict): The attributes of the node.
            labels (Union[str, List[str]]): The label(s) of the node.
        """
        if isinstance(labels, list):
            labels = ":".join(labels)
        now = DateTime.now()
        query = (
            f"MERGE (n:{labels} {{id:$node_id}}) "
            "SET n += $attributes, n.createdAt = $now"
        )
        tx.run(query, node_id=node_id, attributes=attributes, now=now)

        # to create a new case
    # @classmethod
    # def create_new_node(cls, tx, id: str, attr: dict, label: Union[str, list]):
    #     if isinstance(label, list):
    #         label = ":".join(label)
    #     now = DateTime.now()
    #     query = (
    #             "MERGE (n:" + label + " {id:$id})"
    #                                   "SET n += $attr, n.createdAt=$now;"
    #     )
    #     res = tx.run(query, id=id, attr=attr, now=now)
    #     return res

    @classmethod
    def update_existing_node(cls, tx, node_id: str, attributes: dict, labels: Union[str, List[str]]):
        """
        Update the attributes of an existing node with the given ID and labels.

        Args:
            tx: The transaction context.
            node_id (str): The ID of the node.
            attributes (dict): The new attributes of the node.
            labels (Union[str, List[str]]): The label(s) of the node.
        """
        if isinstance(labels, list):
            labels = ":".join(labels)
        now = DateTime.now()
        query = (
            f"MATCH (n:{labels} {{id:$node_id}}) "
            "SET n += $attributes, n.updatedAt = $now"
        )
        tx.run(query, node_id=node_id, attributes=attributes, now=now)

    # @classmethod
    # def update_existing_node(cls, tx, id: str, attr: dict, label: Union[str, list]):
    #     """to update the meta data of an existing case"""
    #     if isinstance(label, list):
    #         label = ":".join(label)
    #     now = DateTime.now()
    #     query = (
    #             "MATCH (n:" + label + " {id:$id})"
    #                                   "SET n += $attr, n.updatedAt=$now;"
    #     )
    #     res = tx.run(query, id=id, attr=attr, now=now)
    #     return res

    @classmethod
    def create_or_update_node(cls, tx, node_id: str, attributes: dict):
        """
        Create a new node or update an existing node with the given ID and attributes.

        Args:
            tx: The transaction context.
            node_id (str): The ID of the node.
            attributes (dict): The attributes of the node.
        """
        # labels = [attributes.get('doctype', 'Case'), attributes.get('label', 'Case')]
        if 'doctype' in attributes.keys():
            labels = [attributes['doctype'], attributes['label']]
        else:
            labels = attributes['label']

        if cls.node_exists(tx, node_id, labels):
            cls.update_existing_node(tx, node_id, attributes, labels)
        else:
            cls.create_new_node(tx, node_id, attributes, labels)
    # @classmethod
    # def create_a_node(cls, tx, id: str, attr: dict):
    #     """either to update an existing case or to create a new case"""
    #     # set the label(s) of the node
    #     if 'doctype' in attr.keys():
    #         label = [attr['doctype'], attr['label']]
    #     else:
    #         label = attr['label']
    #
    #     # check whether the node has already existed
    #     if cls.node_exists(tx, id, label):
    #         res = cls.update_existing_node(tx, id, attr, label)
    #     else:
    #         res = cls.create_new_node(tx, id, attr, label=label)

    @classmethod
    def create_relationship(cls, tx, from_id: str, from_label: str, to_id: str, to_label: str, attributes: dict):
        """
        Create a relationship between two nodes.

        Args:
            tx: The transaction context.
            from_id (str): The ID of the starting node.
            from_label (str): The label of the starting node.
            to_id (str): The ID of the ending node.
            to_label (str): The label of the ending node.
            attributes (dict): The attributes of the relationship.
        """
        query = (
            f"MATCH (n:{from_label} {{id:$from_id}}) "
            f"MATCH (m:{to_label} {{id:$to_id}}) "
            f"MERGE (n)-[r:{from_label.upper()}_TO_{to_label.upper()}]->(m) "
            "SET r += $attributes"
        )
        tx.run(query, from_id=from_id, to_id=to_id, attributes=attributes)

    # @classmethod
    # def create_relationship(cls, tx, From: str, flabel: str, To: str, tlabel: str, attr):
    #     # f = flabel
    #     q = (
    #             "MATCH (n:" + flabel + " {id:$From})"
    #                                    "MATCH (m:" + tlabel + " {id:$To})"
    #                                                           "MERGE (n)-[r:" + flabel.upper() + "_TO_" + tlabel.upper() + "]->(m)"
    #                                                                                                                        "SET r += $attr"
    #     )
    #     res = tx.run(q, From=From, To=To, attr=attr)

    @classmethod
    def create_case_to_case_relationship(cls, tx, from_id: str, to_id: str):
        """
        Create a CASE_TO_CASE relationship between two nodes.

        Args:
            tx: The transaction context.
            from_id (str): The ID of the starting node.
            to_id (str): The ID of the ending node.
        """
        query = (
            "MATCH (n {id:$from_id}) "
            "MATCH (m {label:'Case', id:$to_id}) "
            "MERGE (n)-[:CASE_TO_CASE]->(m)"
        )
        tx.run(query, from_id=from_id, to_id=to_id)

    @classmethod
    # def create_case_to_case_relationship(cls, tx, From, To):
    #     q = (
    #         "MATCH (n {id:$From})"
    #         "MATCH (m {label:'Case', id:$To})"
    #         "MERGE (n)-[:CASE_TO_CASE]->(m)"
    #     )
    #     res = tx.run(q, From=From, To=To)

    def create_graph(self, graph):
        """
        Enter the data from a Networkx graph into the Neo4j database.

        Args:
            graph (networkx.Graph): The Networkx graph containing the data.
        """
        with self.driver.session() as session:
            # Clean the database
            session.write_transaction(self.clean_db)

            # Create nodes
            for node_id, attributes in graph.nodes(data=True):
                session.execute_write(self.create_or_update_node, node_id=node_id, attributes=attributes)

            # Create relationships
            for start_node_id, end_node_id, attributes in graph.edges(data=True):
                # if attributes['label'] == 'CASE_TO_CASE':
                # REMOVE DUPLICATES
                session.execute_write(
                    self.create_relationship,
                    from_id=start_node_id,
                    from_label='Case',
                    to_id=end_node_id,
                    to_label='Case',
                    attributes=attributes
                )
                # elif attributes['label'] == 'DUPLICATE_TO_CASE':
                #     session.execute_write(
                #         self.create_relationship,
                #         start_node_id=start_node_id,
                #         start_label='Duplicate',
                #         end_node_id=end_node_id,
                #         end_label='Case',
                #         attributes={}
                #     )

        # Close the driver
        self.driver.close()
