
import pandas as pd
from rdflib import Graph, URIRef, Literal, RDFS, RDF
import rdflib
import duckdb

def create_rdf_graph(df):
    graph = rdflib.Graph()
    namespace_data_graph = "http://www.slb.com/data-graphs/Troubleshooting_ORA_FNFM_Data_graph#"
    namespace_ontology = "http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#"

    for index, row in df.iterrows():
        col1_uri = URIRef(namespace_data_graph + str(row.iloc[0]).replace(" ", "_"))
        col2_uri = URIRef(namespace_data_graph + str(row.iloc[1]).replace(" ", "_"))
        col3_uri = URIRef(namespace_data_graph + str(row.iloc[2]).replace(" ", "_"))
        col4_uri = URIRef(namespace_data_graph + str(row.iloc[3]).replace(" ", "_"))
        col5_uri = URIRef(namespace_data_graph + str(row.iloc[4]).replace(" ", "_"))
        col6_uri = URIRef(namespace_data_graph + str(row.iloc[5]).replace(" ", "_"))

        graph.add((col1_uri, RDF.type, URIRef(namespace_ontology + "Failure")))
        graph.add((col2_uri, RDF.type, URIRef(namespace_ontology + "Failure")))
        graph.add((col3_uri, RDF.type, URIRef(namespace_ontology + "RootCause")))
        graph.add((col4_uri, RDF.type, URIRef(namespace_ontology + "RootCause")))
        graph.add((col5_uri, RDF.type, URIRef(namespace_ontology + "Trigger")))
        graph.add((col6_uri, RDF.type, URIRef(namespace_ontology + "DataChannel")))

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
    # Read Excel file with header=1 and specific columns like in your original code
    df = pd.read_excel(xlsx_path, header=1, usecols=range(1, 7))
    graph = create_rdf_graph(df)
    graph.serialize(destination=output_path, format='turtle')
    return output_path

def load_knowledge_graph(file_path):
    """Load the knowledge graph from TTL file"""
    g = Graph()
    g.parse(file_path, format='turtle')
    return g

def get_failures_from_kg(g):
    """Get all failures from the knowledge graph"""
    query = """
    PREFIX troubleshooting_ora_fnfm_ontology_: <http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?failure
    WHERE {
      ?failure_uri a troubleshooting_ora_fnfm_ontology_:Failure ;
      rdfs:label ?failure
    }
    """
    failure_query = g.query(query)
    df = pd.DataFrame(failure_query, columns=["failure"])
    return df["failure"].tolist()

def execute_query_for_concept(g, concept):
    """Return the triples related to a specific concept."""
    query = f"""
    PREFIX arg: <http://spinrdf.org/arg#>
    PREFIX dash: <http://datashapes.org/dash#>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX edg: <http://edg.topbraid.solutions/model/>
    PREFIX graphql: <http://datashapes.org/graphql#>
    PREFIX metadata: <http://topbraid.org/metadata#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX sh: <http://www.w3.org/ns/shacl#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
    PREFIX smf: <http://topbraid.org/sparqlmotionfunctions#>
    PREFIX spl: <http://spinrdf.org/spl#>
    PREFIX swa: <http://topbraid.org/swa#>
    PREFIX teamwork: <http://topbraid.org/teamwork#>
    PREFIX teamworkconstraints: <http://topbraid.org/teamworkconstraints#>
    PREFIX tosh: <http://topbraid.org/tosh#>
    PREFIX troubleshooting_ora_fnfm_data_graph: <http://www.slb.com/data-graphs/Troubleshooting_ORA_FNFM_Data_graph#>
    PREFIX troubleshooting_ora_fnfm_ontology_: <http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

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
    """Return a dictionary of lists of triples for each depth for a specified concept."""
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

def recursive_execute_function(dict_tuple_result, partition_id):
    """Recursive execution of all functions"""
    from .teradata_utils import execute_function_from_the_map
    
    result_list = []
    all_tuples = [t for tuples in dict_tuple_result.values() for t in tuples]
    df_tuples = pd.DataFrame(all_tuples, columns=['Subject', 'Predicate', 'Object'])
    
    query_trigger_datachannel = """
    SELECT DISTINCT t1.Object AS Trigger, t2.predicate AS Consume, t2.object AS DataChannel 
    FROM df_tuples t1
    JOIN df_tuples t2 ON t1.Object = t2.Subject
    WHERE t1.Predicate = 'isTriggeredBy' AND t2.predicate = 'consume'
    """
    result_df = duckdb.query(query_trigger_datachannel).to_df()
    
    for index, row in result_df.iterrows():
        function = row.iloc[0] 
        consume = row.iloc[1]
        datachannel = row.iloc[2]  
        result = execute_function_from_the_map(function, partition_id, datachannel)
        result_list.append((function, consume, datachannel, result))
    
    return pd.DataFrame(result_list, columns=['Subject', 'Predicate', 'Object', 'Status'])

def analyze_root_causes(df_clean, failure_selectbox):
    """Analyze root causes and return alert data"""
    table_data = []
    
    if failure_selectbox:
        query_rootcause = f"""
            SELECT Object 
            FROM df_clean
            WHERE Subject = '{failure_selectbox}' AND Predicate = 'hasRootCause'
        """
        try:
            rootcause_df = duckdb.query(query_rootcause).to_df()  
            if not rootcause_df.empty:
                for root_cause in rootcause_df["Object"]:
                    query_trigger = f"""
                        SELECT Object
                        FROM df_clean
                        WHERE Subject = '{root_cause}' AND Predicate = 'isTriggeredBy'
                    """
                    trigger_df = duckdb.query(query_trigger).to_df()

                    if not trigger_df.empty:
                        for trigger_value in trigger_df["Object"]:
                            query_datachannel = f"""
                                SELECT DISTINCT Object, Status
                                FROM df_clean 
                                WHERE Subject='{trigger_value}' AND Predicate='consume' AND Status=True
                            """
                            datachannel_df = duckdb.query(query_datachannel).to_df()

                            if not datachannel_df.empty:
                                for _, row in datachannel_df.iterrows():
                                    table_data.append([root_cause, trigger_value, row['Object']])
        except Exception as e:
            print(f"Error analyzing root causes: {e}")
    
    return table_data


