from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import pandas as pd
from rdflib import Graph, Literal, URIRef, RDFS
import os
from django.shortcuts import render
from pathlib import Path

# Utility imports
from .teradata_utils import get_metadata, get_partition_id, run_checks
from .kg_utils import execute_query_for_concept, graph_search_tuple

# ==============================
# Knowledge Graph Loading
# ==============================
KG_FILE = r"C:\Users\AChaliha\Desktop\StreamlitToDjango\troubleshooting\output_ORA_FNFM_KG.ttl"

g = Graph()
try:
    # ✅ Safer way to load local file as URI
    g.parse(Path(KG_FILE).as_uri(), format='turtle')
except Exception as e:
    print(f"Error loading Knowledge Graph: {e}")


# ==============================
# API Views
# ==============================

class FailureListView(APIView):
    def get(self, request):
        # Fetch all failures from KG
        query = """
        PREFIX troubleshooting_ora_fnfm_ontology_: <http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT DISTINCT ?failure
        WHERE {
          ?failure_uri a troubleshooting_ora_fnfm_ontology_:Failure ;
          rdfs:label ?failure
        }
        """
        results = g.query(query)
        failures = [str(row[0]) for row in results]
        return Response({'failures': failures})


class MetadataView(APIView):
    def get(self, request):
        df_metadata = get_metadata()
        serials = sorted(df_metadata["serial_number"].fillna('NaN').unique())
        return Response({'serial_numbers': serials})


class PartitionView(APIView):
    def get(self, request):
        serial = request.query_params.get('serial_number')
        job = request.query_params.get('job_number')
        start_job = request.query_params.get('start_job')
        partition_id = get_partition_id(serial, job, start_job)
        return Response({'partition_id': partition_id})


class ChecksView(APIView):
    def post(self, request):
        partition_id = request.data.get('partition_id')
        datachannel = request.data.get('datachannel')
        check_type = request.data.get('check_type')
        status_result = run_checks(partition_id, datachannel, check_type)
        return Response({'status': status_result})


class RootCauseView(APIView):
    def get(self, request):
        failure = request.query_params.get('failure')
        partition_id = request.query_params.get('partition_id')
        # Placeholder: implement logic later
        return Response({'rootcause_table': []})


class GraphView(APIView):
    def get(self, request):
        failure = request.query_params.get('failure')
        partition_id = request.query_params.get('partition_id')
        # Placeholder: implement logic later
        return Response({'nodes': [], 'edges': []})


def index(request):
    """Renders main troubleshooting UI."""
    return render(request, "troubleshooting/index.html")


# ==============================
# Extra Utility Functions
# ==============================

def load_kg(file_path):
    graph = Graph()
    graph.parse(Path(file_path).as_uri(), format='turtle')
    return graph


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
    labels_list = [(str(row[0]), str(row[1]), str(row[2])) for row in result]
    return labels_list


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


# Create your views here.
