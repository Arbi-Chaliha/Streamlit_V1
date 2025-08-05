
import pandas as pd
import json
import os
import numpy as np
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .kg_utils import (
    load_knowledge_graph, get_failures_from_kg, 
    graph_search_tuple, recursive_execute_function, analyze_root_causes
)
from .teradata_utils import get_metadata, get_partition_id

def convert_numpy_types(obj):
    """Convert numpy types to native Python types for JSON serialization"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif pd.isna(obj):
        return None
    else:
        return obj

def index(request):
    """Main troubleshooting interface"""
    # Load knowledge graph
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    kg_path = os.path.join(BASE_DIR, 'troubleshooting', 'output_ORA_FNFM_KG.ttl')
    
    if not os.path.exists(kg_path):
        return render(request, 'troubleshooting/error.html', {
            'error': 'Knowledge graph not found. Please build it first.'
        })
    
    g = load_knowledge_graph(kg_path)
    failures = get_failures_from_kg(g)
    
    # Get metadata for dropdowns
    try:
        df_metadata = get_metadata()
        serial_numbers = sorted(df_metadata["serial_number"].fillna('NaN').unique())
        # Convert numpy array to Python list with proper type conversion
        serial_numbers = [convert_numpy_types(sn) for sn in serial_numbers]
    except Exception as e:
        return render(request, 'troubleshooting/error.html', {
            'error': f'Database connection error: {str(e)}'
        })
    
    context = {
        'failures': failures,
        'serial_numbers': serial_numbers,
    }
    return render(request, 'troubleshooting/index.html', context)

@csrf_exempt
def get_job_numbers(request):
    """Get job numbers for a selected serial number"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            serial_number = data.get('serial_number')
            
            if not serial_number:
                return JsonResponse({'error': 'Serial number is required'}, status=400)
            
            df_metadata = get_metadata()
            df_serial = df_metadata[df_metadata["serial_number"] == serial_number]
            
            if df_serial.empty:
                return JsonResponse({'job_numbers': []})
            
            job_numbers = sorted(df_serial["job_number"].fillna('NaN').unique())
            # Convert numpy array to Python list with proper type conversion
            job_numbers_list = [convert_numpy_types(job) for job in job_numbers]
            
            return JsonResponse({'job_numbers': job_numbers_list})
            
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)

@csrf_exempt
def get_start_jobs(request):
    """Get start jobs for selected serial number and job number"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            serial_number = data.get('serial_number')
            job_number = data.get('job_number')
            
            if not serial_number or not job_number:
                return JsonResponse({'error': 'Serial number and job number are required'}, status=400)
            
            df_metadata = get_metadata()
            df_filtered = df_metadata[
                (df_metadata["serial_number"] == serial_number) &
                (df_metadata["job_number"] == job_number)
            ]
            
            if df_filtered.empty:
                return JsonResponse({'start_jobs': []})
            
            start_jobs = sorted(df_filtered["job_start"].fillna('NaN').unique())
            # Convert numpy array to Python list with proper type conversion
            start_jobs_list = []
            for job in start_jobs:
                if pd.isna(job):
                    start_jobs_list.append('NaN')
                else:
                    # Convert to string and handle numpy types
                    converted_job = convert_numpy_types(job)
                    start_jobs_list.append(str(converted_job))
            
            return JsonResponse({'start_jobs': start_jobs_list})
            
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)

@csrf_exempt
def analyze_failure(request):
    """Analyze failure and return results"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            failure = data.get('failure')
            serial_number = data.get('serial_number')
            job_number = data.get('job_number')
            start_job = data.get('start_job')
            
            # Validate required fields
            if not all([failure, serial_number, job_number, start_job]):
                return JsonResponse({
                    'error': 'All fields are required: failure, serial_number, job_number, start_job'
                }, status=400)
            
            # Get partition ID
            partition_id = get_partition_id(serial_number, job_number, start_job)
            if not partition_id:
                return JsonResponse({'error': 'Partition ID not found'}, status=404)
            
            # Load knowledge graph and analyze
            BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            kg_path = os.path.join(BASE_DIR, 'troubleshooting', 'output_ORA_FNFM_KG.ttl')
            
            if not os.path.exists(kg_path):
                return JsonResponse({'error': 'Knowledge graph file not found'}, status=500)
            
            g = load_knowledge_graph(kg_path)
            
            # Get tuples for the failure
            dic_tuple_result = graph_search_tuple(g, failure, max_depth=-1)
            
            if not dic_tuple_result:
                return JsonResponse({
                    'partition_id': partition_id,
                    'alerts': [],
                    'graph_data': []
                })
            
            # Execute functions
            result = recursive_execute_function(dic_tuple_result, partition_id)
            
            # Merge with original tuples
            all_tuples = [t for tuples in dic_tuple_result.values() for t in tuples]
            df_tuples = pd.DataFrame(all_tuples, columns=['Subject', 'Predicate', 'Object'])
            df_final = pd.merge(df_tuples, result, on=["Subject", "Predicate", "Object"], how="left")
            df_clean = df_final[df_final["Status"].apply(lambda x: x is not None)]
            
            # Analyze root causes
            alerts = analyze_root_causes(df_clean, failure)
            
            # Convert DataFrame to dict for JSON serialization
            graph_data = []
            if not df_clean.empty:
                # Convert all values to JSON-serializable types
                graph_data_raw = df_clean.to_dict('records')
                graph_data = []
                for record in graph_data_raw:
                    converted_record = {}
                    for key, value in record.items():
                        converted_record[key] = convert_numpy_types(value)
                    graph_data.append(converted_record)
            
            # Convert partition_id to native Python type
            partition_id_native = convert_numpy_types(partition_id)
            
            # Convert alerts to ensure JSON serializability
            alerts_converted = []
            for alert in alerts:
                if isinstance(alert, (list, tuple)):
                    alerts_converted.append([convert_numpy_types(item) for item in alert])
                else:
                    alerts_converted.append(convert_numpy_types(alert))
            
            return JsonResponse({
                'partition_id': partition_id_native,
                'alerts': alerts_converted,
                'graph_data': graph_data
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'error': f'Analysis failed: {str(e)}'}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)

