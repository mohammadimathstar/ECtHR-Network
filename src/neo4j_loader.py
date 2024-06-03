from neo4j.time import DateTime
from neo4j import GraphDatabase
from typing import Union
import json
import networkx as nx


def from_json(fname: str='graphJUD'):
    if fname[-4:] not in ".json":
        fname = fname + ".json"
    with open(fname, 'r') as f:
        data = json.load(f)
    G = nx.json_graph.node_link_graph(data)
    return G

class App:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    # to clean the database
    @classmethod
    def clean_db(cls, tx):
        query = (
            "MATCH (n) DETACH DELETE n"
        )
        tx.run(query)

    # to check whether a case exists or not
    @classmethod
    def node_exists(cls, tx, id: str, label: Union[str, list]):
        if isinstance(label, list):
            label = ":".join(label)
        query = (
            "OPTIONAL MATCH (n:" + label + " {id:$id})"
            "RETURN n IS NOT NULL"
        )
        res = tx.run(query,  id=id)
        return res.single()[0]


    # to create a new case
    @classmethod
    def create_new_node(cls, tx, id: str, attr: dict, label: Union[str, list]):
        if isinstance(label, list):
            label = ":".join(label)
        now = DateTime.now()
        query = (
            "MERGE (n:" + label + " {id:$id})"
            "SET n += $attr, n.createdAt=$now;"
        )
        res = tx.run(query, id=id, attr=attr, now=now)
        return res

    # to update the meta data of an existing case
    @classmethod
    def update_existing_node(cls, tx, id:str, attr:dict, label: Union[str, list]):
        if isinstance(label, list):
            label = ":".join(label)
        now = DateTime.now()
        query = (
            "MATCH (n:" + label + " {id:$id})"
            "SET n += $attr, n.updatedAt=$now;"
        )
        res = tx.run(query, id=id, attr=attr, now=now)
        return res

    # either to update an existing case or to create a new case
    @classmethod
    def create_a_node(cls, tx, id:str, attr: dict):
        # set the label(s) of the node
        if 'doctype' in attr.keys():
            label= [attr['doctype'], attr['label']]
        else:
            label = attr['label']

        # check whether the node has already existed
        if cls.node_exists(tx, id, label):
            res = cls.update_existing_node(tx, id, attr, label)
        else:
            res = cls.create_new_node(tx, id, attr, label=label)


    @classmethod
    def create_relationship(cls, tx, From: str, flabel: str, To: str, tlabel: str, attr):
        # f = flabel
        q = (
            "MATCH (n:" + flabel + " {id:$From})"
            "MATCH (m:" + tlabel + " {id:$To})"
            "MERGE (n)-[r:" + flabel.upper() + "_TO_" + tlabel.upper() + "]->(m)"
            "SET r += $attr"
        )
        res = tx.run(q, From=From, To=To, attr=attr)

    @classmethod
    def create_case_to_case_relationship(cls, tx, From, To):
        q = (
            "MATCH (n {id:$From})"
            "MATCH (m {label:'Case', id:$To})"
            "MERGE (n)-[:CASE_TO_CASE]->(m)"
        )
        res = tx.run(q, From=From, To=To)
        # info = res.consume()
        # print("{0} edge created".format(info.counters.relationships_created))
        # print("{0} properties of the edge set".format(info.counters.properties_set))

    @classmethod
    def create_case_to_legislation_relationship(cls, tx, From, To):
        q = (
            "MATCH (n {label:'Case', id:$From})"
            "MATCH (m {label:'Law', id:$To})"
            "MERGE (n)-[:CASE_TO_LAW]->(m)"
        )
        res = tx.run(q, From=From, To=To)

    @classmethod
    def search_a_list(cls, tx, ids: list):
        query = (
            """
            MATCH (n)
            WHERE n.id IN {0}
            RETURN n;
            """.format(ids)
        )
        res = tx.run(query, ids=ids)
        # print([r for r in res])
        # print(res.peek())

    @classmethod
    def search_evict(cls, tx):
        query = (
            """
            MATCH (n)
            WHERE n.label = 'Case' AND n.topic='Evict'
            RETURN n
            """
        )
        res = tx.run(query)
        print(res.peek())


    @classmethod
    def get_db(cls, tx):
        query = (
            """
            MATCH (n)
            RETURN n
            """
        )
        res = tx.run(query)

    @classmethod
    def get_outdegree(cls, tx, Gname):
        tx.run("CALL gds.degree.write($Gname, {writeProperty: 'outgoing citations'})", Gname=Gname)


    @classmethod
    def get_indegree(cls, tx, Gname):
        tx.run("CALL gds.degree.write($Gname, {writeProperty: 'incoming citations', orientation: 'REVERSE'})", Gname=Gname)

    @classmethod
    def get_betweenness(cls, tx, Gname):
        tx.run("CALL gds.degree.write($Gname, {writeProperty: 'betweenness'})", Gname=Gname)

    @classmethod
    def get_pagerank(cls, tx, Gname):
        tx.run(
            """
            CALL gds.pageRank.write($Gname, {
            nodeLabels: ['Case'],
            relationshipTypes: ['CASE_TO_CASE'],
            scaler: 'L1Norm',
            maxIterations: 20,
            dampingFactor: 0.85,
            writeProperty: 'pagerank'})
            """
            , Gname=Gname
        )

    @classmethod
    def get_articlerank(cls, tx, Gname):
        tx.run(
            """
            CALL gds.articleRank.write($Gname, {
            nodeLabels: ['Case'],
            relationshipTypes: ['CASE_TO_CASE'],
            scaler: 'L1Norm',
            maxIterations: 20,
            dampingFactor: 0.85,
            writeProperty: 'articlerank'})
            """
            , Gname=Gname
        )

    @classmethod
    def get_influence(cls, tx, Gname):
        tx.run(
            """
            CALL gds.beta.influenceMaximization.celf.write($Gname, {
            writeProperty: 'celfSpread',
            seedSetSize: 3
            })
            """
            , Gname=Gname
        )

    @classmethod
    def get_closeness(cls, tx, Gname):
        tx.run(
            """
            CALL gds.beta.closeness.write($Gname, {writeProperty: 'closeness'})
            """
            , Gname=Gname
        )

    @classmethod
    def get_hits(cls, tx, Gname):
        # The Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm
        tx.run(
            """
            CALL gds.alpha.hits.write($Gname, {writeProperty: 'hits', hitsIterations:10})
            """
            , Gname=Gname
        )

    @classmethod
    def louvain_com_detection(cls, tx, Gname):
        tx.run(
            """
            CALL gds.louvain.write($Gname, {writeProperty: 'community', maxLevels: 20})
            """
            , Gname=Gname
        )


    def entering_data(self, G):
        # ids = ['30078/06:2012', '17120/09:2014'] #["Ali", "Ali Ahmad"]
        with self.driver.session() as session:

            session.write_transaction(self.clean_db)
            for n, d in G.nodes(data=True):
                session.execute_write(self.create_a_node, id=n, attr=d)

            for (s, t, d) in G.edges(data=True):
                if d['label']=='CASE_TO_CASE':
                    session.execute_write(self.create_relationship, From=s, flabel='Case', To=t, tlabel='Case', attr=d)
                elif d['label']=='DUPLICATE_TO_CASE':
                    session.execute_write(self.create_relationship, From=s, flabel='Duplicate', To=t, tlabel='Case', attr=dict())

        self.driver.close()


    def apply_community_detection(self):
        graph_name = 'myGraph_com'
        with self.driver.session() as session:
            session.run("CALL gds.graph.drop($name, false)", name=graph_name)
            session.run("CALL gds.graph.project($name, 'Case', 'CASE_TO_CASE')", name=graph_name)
            session.execute_write(self.louvain_com_detection, Gname=graph_name)

    def compute_centralities(self, centrality_measure='outdegree'):
        centrality_measures = {'indegree': self.get_indegree, 'outdegree': self.get_outdegree,
                               'betweenness': self.get_betweenness,
                               'closeness': self.get_closeness, 'pagerank': self.get_pagerank,
                               'articlerank': self.get_articlerank,
                               'influence': self.get_influence, 'hits': self.get_hits}
        assert centrality_measure in centrality_measures.keys(), f'{centrality_measure} is not allowed.'


        graph_name = 'myGraph'
        with self.driver.session() as session:
            session.run("CALL gds.graph.drop($name, false)", name=graph_name)
            session.run("CALL gds.graph.drop($name, false)", name=graph_name+"_r")

            session.run("CALL gds.graph.project($name, 'Case', 'CASE_TO_CASE')", name=graph_name)
            session.run("CALL gds.graph.project($name, 'Case', 'CASE_TO_CASE')", name=graph_name+"_r")

            centrality = d[centrality_measure]#globals()['self.get_' + centrality_measure]
            if centrality_measure in ['indegree']:
                session.execute_write(centrality, Gname=graph_name+'_r')
            else:
                session.execute_write(centrality, Gname=graph_name)

            # get outdegree
            # session.execute_write(self.get_outdegree, Gname=graph_name)

            # get indegree
            # session.execute_write(self.get_indegree, Gname=graph_name+"_r")

            # # get betweenness
            # session.execute_write(self.get_betweenness, Gname=graph_name)
            #
            # # get closeness
            # session.execute_write(self.get_closeness, Gname=graph_name)
            #
            # # get pagerank
            # session.execute_write(self.get_pagerank, Gname=graph_name)
            #
            # # get articlerank
            # session.execute_write(self.get_articlerank, Gname=graph_name)
            #
            # # get get_in
            # session.execute_write(self.get_influence, Gname=graph_name)
            #
            # # get hits
            # session.execute_write(self.get_hits, Gname=graph_name)
            #
            # # get hits
            # session.execute_write(self.louvain_com_detection, Gname=graph_name)

class App_community:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    # to clean the database
    @classmethod
    def clean_db(cls, tx):
        query = (
            "MATCH (n) DETACH DELETE n"
        )
        tx.run(query)

    # to check whether a case exists or not
    @classmethod
    def node_exists(cls, tx, id: str):
        query = (
            "OPTIONAL MATCH (n {id:$id})"
            "RETURN n IS NOT NULL"
        )
        res = tx.run(query,  id=id)
        return res.single()[0]


    # to create a new case
    @classmethod
    def create_new_node(cls, tx, id: str, attr: dict):

        now = DateTime.now()
        query = (
            "MERGE (n {id:$id})"
            "SET n += $attr, n.createdAt=$now;"
        )
        res = tx.run(query, id=id, attr=attr, now=now)
        return res

    # to update the meta data of an existing case
    @classmethod
    def update_existing_node(cls, tx, id:str, attr:dict):

        now = DateTime.now()
        query = (
            "MATCH (n {id:$id})"
            "SET n += $attr, n.updatedAt=$now;"
        )
        res = tx.run(query, id=id, attr=attr, now=now)
        return res

    # either to update an existing case or to create a new case
    @classmethod
    def create_a_node(cls, tx, id:str, attr: dict):

        # check whether the node has already existed
        if cls.node_exists(tx, id):
            res = cls.update_existing_node(tx, id, attr)
        else:
            res = cls.create_new_node(tx, id, attr)


    @classmethod
    def create_relationship(cls, tx, From: str, To: str, attr):
        q = (
            "MATCH (n {id:$From})"
            "MATCH (m {id:$To})"
            "MERGE (n)-[r:COM_TO_COM]->(m)"
            "SET r += $attr"
        )

        res = tx.run(q, From=From, To=To, attr=attr)

    @classmethod
    def get_db(cls, tx):
        query = (
            """
            MATCH (n)
            RETURN n
            """
        )
        res = tx.run(query)

    @classmethod
    def get_outdegree(cls, tx, Gname):
        tx.run("CALL gds.degree.write($Gname, {writeProperty: 'outgoing citations'})", Gname=Gname)


    @classmethod
    def get_indegree(cls, tx, Gname):
        tx.run("CALL gds.degree.write($Gname, {writeProperty: 'incoming citations', orientation: 'REVERSE'})", Gname=Gname)

    @classmethod
    def get_betweenness(cls, tx, Gname):
        tx.run("CALL gds.degree.write($Gname, {writeProperty: 'betweenness'})", Gname=Gname)

    @classmethod
    def get_pagerank(cls, tx, Gname):
        tx.run(
            """
            CALL gds.pageRank.write($Gname, {
            nodeLabels: ['Case'],
            relationshipTypes: ['CASE_TO_CASE'],
            scaler: 'L1Norm',
            maxIterations: 20,
            dampingFactor: 0.85,
            writeProperty: 'pagerank'})
            """
            , Gname=Gname
        )

    @classmethod
    def get_articlerank(cls, tx, Gname):
        tx.run(
            """
            CALL gds.articleRank.write($Gname, {
            nodeLabels: ['Case'],
            relationshipTypes: ['CASE_TO_CASE'],
            scaler: 'L1Norm',
            maxIterations: 20,
            dampingFactor: 0.85,
            writeProperty: 'articlerank'})
            """
            , Gname=Gname
        )

    @classmethod
    def get_influence(cls, tx, Gname):
        tx.run(
            """
            CALL gds.beta.influenceMaximization.celf.write($Gname, {
            writeProperty: 'celfSpread',
            seedSetSize: 3
            })
            """
            , Gname=Gname
        )

    @classmethod
    def get_closeness(cls, tx, Gname):
        tx.run(
            """
            CALL gds.beta.closeness.write($Gname, {writeProperty: 'closeness'})
            """
            , Gname=Gname
        )

    @classmethod
    def get_hits(cls, tx, Gname):
        # The Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm
        tx.run(
            """
            CALL gds.alpha.hits.write($Gname, {writeProperty: 'hits', hitsIterations:10})
            """
            , Gname=Gname
        )

    def entering_data(self, G):

        with self.driver.session() as session:

            session.write_transaction(self.clean_db)
            for n, d in G.nodes(data=True):
                session.execute_write(self.create_a_node, id=n, attr=d)

            for (s, t, d) in G.edges(data=True):
                session.execute_write(self.create_relationship, From=s, To=t, attr=d)


        self.driver.close()

    def compute_centralities(self, centrality_measure='outdegree'):
        centrality_measures = {'indegree': self.get_indegree, 'outdegree': self.get_outdegree,
                               'betweenness': self.get_betweenness,
                               'closeness': self.get_closeness, 'pagerank': self.get_pagerank,
                               'articlerank': self.get_articlerank,
                               'influence': self.get_influence, 'hits': self.get_hits}
        assert centrality_measure in centrality_measures.keys(), f'{centrality_measure} is not allowed.'


        graph_name = 'myGraph'
        with self.driver.session() as session:
            session.run("CALL gds.graph.drop($name, false)", name=graph_name)
            session.run("CALL gds.graph.drop($name, false)", name=graph_name+"_r")

            session.run("CALL gds.graph.project($name, 'Case', 'CASE_TO_CASE')", name=graph_name)
            session.run("CALL gds.graph.project($name, 'Case', 'CASE_TO_CASE')", name=graph_name+"_r")

            centrality = d[centrality_measure]#globals()['self.get_' + centrality_measure]
            if centrality_measure in ['indegree']:
                session.execute_write(centrality, Gname=graph_name+'_r')
            else:
                session.execute_write(centrality, Gname=graph_name)


