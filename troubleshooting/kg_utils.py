import pandas as pd
from rdflib import Graph, URIRef, Literal, RDFS
import rdflib


def create_rdf_graph(df):
    graph = rdflib.Graph()
    namespace_data_graph = URIRef("http://www.slb.com/data-graphs/Troubleshooting_ORA_FNFM_Data_graph#")
    namespace_ontology = URIRef("http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#")

    for index, row in df.iterrows():
        col1_uri = URIRef(namespace_data_graph + str(row.iloc[0]).replace(" ", "_"))
        col2_uri = URIRef(namespace_data_graph + str(row.iloc[1]).replace(" ", "_"))
        col3_uri = URIRef(namespace_data_graph + str(row.iloc[2]).replace(" ", "_"))
        col4_uri = URIRef(namespace_data_graph + str(row.iloc[3]).replace(" ", "_"))
        col5_uri = URIRef(namespace_data_graph + str(row.iloc[4]).replace(" ", "_"))
        col6_uri = URIRef(namespace_data_graph + str(row.iloc[5]).replace(" ", "_"))

        graph.add((col1_uri, rdflib.RDF.type, URIRef(namespace_ontology + "Failure")))
        graph.add((col2_uri, rdflib.RDF.type, URIRef(namespace_ontology + "Failure")))
        graph.add((col3_uri, rdflib.RDF.type, URIRef(namespace_ontology + "RootCause")))
        graph.add((col4_uri, rdflib.RDF.type, URIRef(namespace_ontology + "RootCause")))
        graph.add((col5_uri, rdflib.RDF.type, URIRef(namespace_ontology + "Trigger")))
        graph.add((col6_uri, rdflib.RDF.type, URIRef(namespace_ontology + "DataChannel")))

        graph.add((col1_uri, RDFS.label, Literal(str(row.iloc[0]))))
        graph.add((col2_uri, RDFS.label, Literal(str(row.iloc[1]))))
        graph.add((col3_uri, RDFS.label, Literal(str(row.iloc[2]))))
        graph.add((col4_uri, RDFS.label, Literal(str(row.iloc[3]))))
        graph.add((col5_uri, RDFS.label, Literal(str(row.iloc[4]))))
        graph.add((col6_uri, RDFS.label, Literal(str(row.iloc[5]))))

        graph.add((col1_uri, URIRef(namespace_ontology + "cause"), col2_uri))
        graph.add((col1_uri, URIRef(namespace_ontology + "hasRootCause"), col3_uri))
        graph.add((col3_uri, URIRef(namespace_ontology + "next"), col4_uri))
        graph.add((col3_uri, URIRef(namespace_ontology + "isTriggeredBy"), col5_uri))
        graph.add((col5_uri, URIRef(namespace_ontology + "consume"), col6_uri))

    return graph

def create_rdf_graph_from_xlsx(xlsx_path, output_path):
    df = pd.read_excel(xlsx_path)
    graph = create_rdf_graph(df)
    graph.serialize(destination=output_path, format='turtle')
def execute_query_for_concept(g, concept):
    query = f"""
    PREFIX troubleshooting_ora_fnfm_ontology_: <http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?subject_label (STRAFTER(STR(?predicate), "#") AS ?predicateName) ?object_label
    WHERE {{
        ?subject_uri ?predicate ?object_uri;
                rdfs:label "{concept}";
                rdfs:label ?subject_label.
        ?subject_uri a ?type_subject.
        ?object_uri a ?type_object;
                rdfs:label ?object_label.
        FILTER (?type_subject IN (troubleshooting_ora_fnfm_ontology_:Failure, troubleshooting_ora_fnfm_ontology_:RootCause, troubleshooting_ora_fnfm_ontology_:Trigger, troubleshooting_ora_fnfm_ontology_:DataChannel) || 
            ?type_object IN (troubleshooting_ora_fnfm_ontology_:Failure, troubleshooting_ora_fnfm_ontology_:RootCause, troubleshooting_ora_fnfm_ontology_:Trigger, troubleshooting_ora_fnfm_ontology_:DataChannel))
        FILTER (?predicate != rdf:type)
    }}
    """
    result = g.query(query)
    return [(str(row[0]), str(row[1]), str(row[2])) for row in result]


def graph_search_tuple(g, concept, visited=None, result=None, max_depth=-1, depth=0, depth_results=None):
    if visited is None:
        visited = []
    if result is None:
        result = []
    if depth_results is None:
        depth_results = {}

    if max_depth != -1 and depth >= max_depth:
        return depth_results
    if concept in visited:
        return depth_results

    visited.append(concept)
    results_query = execute_query_for_concept(g, concept)

    if depth not in depth_results:
        depth_results[depth] = []
    depth_results[depth].extend([elem for elem in results_query if elem not in depth_results[depth]])
    result.extend([elem for elem in results_query if elem not in result])

    for concept1, message, concept2 in results_query:
        graph_search_tuple(g, concept2, visited, result, max_depth, depth + 1, depth_results)

    return depth_results
