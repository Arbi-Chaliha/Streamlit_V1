
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import urllib.parse


load_dotenv()

def get_engine():
    user = os.getenv("TERADATA_USER")
    pasw = os.getenv("TERADATA_PASS")
    host = os.getenv("TERADATA_HOST")
    port = os.getenv("TERADATA_PORT")
    encoded_pass = urllib.parse.quote_plus(pasw)
    return create_engine(f'teradatasql://{user}:{encoded_pass}@{host}/?encryptdata=true')
   
def get_metadata():
    engine = get_engine()
    sql_metadata = "sel * from PRD_RP_PRODUCT_VIEW.FNFM_FLEET_METADATA"
    df_metadata = pd.read_sql(sql_metadata, engine)
    return df_metadata

def get_partition_id(serial_number, job_number, start_job):
    engine = get_engine()
    sql = f"""
    sel partition_id
    from PRD_RP_PRODUCT_VIEW.FNFM_FLEET_METADATA
    where
    serial_number = '{serial_number}' and
    job_number = '{job_number}' and
    CAST(job_start AS CHAR(26)) = '{start_job}';
    """
    df = pd.read_sql(sql, engine)
    if not df.empty:
        return df.iloc[0, 0]
    return None

# All the check functions from your Streamlit app
def threshold_sup_10450(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel sum(error_count) as count_of_error 
    from PRD_RP_PRODUCT_VIEW.FNFM_LIMIT_CHECK_PER_JOB 
    where xcol = 'MCDIGVLTFM' and (metric_name = 'above_sigma_one' 
    or metric_name = 'below_sigma_one') and partition_id = {partition_id}"""
    df = pd.read_sql(sql, engine)
    result_value = df.iloc[0, 0] 
    return result_value is not None and result_value > 10450

def threshold_sup_12000(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel sum(error_count) as sum_error_count
    from PRD_RP_PRODUCT_VIEW.FNFM_LIMIT_CHECK_PER_JOB 
    where xcol = 'MCREFVLTFM' and partition_id = {partition_id} """
    df = pd.read_sql(sql, engine)
    result_value = df.iloc[0, 0]
    return result_value is not None and result_value > 12000

def threshold_sup_5000(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel sum(error_count) as sum_error_count
    from PRD_RP_PRODUCT_VIEW.FNFM_LIMIT_CHECK_PER_JOB 
    where (metric_name = 'above_sigma_one' or metric_name = 'below_sigma_one') and xcol = 'MCINVLTFM' and partition_id = {partition_id} """
    df = pd.read_sql(sql, engine)
    result_value = df.iloc[0, 0] 
    return result_value is not None and result_value > 5000

def discrete_sup_10(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel sum(count_error) as count_of_error
    from PRD_RP_PRODUCT_VIEW.FNFM_STATUS_WORDS_AGGREGATED_PER_JOB
    where xcol = '{triple_subject}' and xcol_decoded = 'FNFM_TripPhaseAFM' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, engine)
    result_value = df.iloc[0, 0]
    return result_value is not None and int(result_value) > 10

def discrete_sup_20(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel sum(count_error) as count_of_error
    from PRD_RP_PRODUCT_VIEW.FNFM_STATUS_WORDS_AGGREGATED_PER_JOB
    where xcol = '{triple_subject}' and xcol_decoded = 'FNFM_EIPUplinkMessageSend' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, engine)
    result_value = df.iloc[0, 0]
    return result_value is not None and int(result_value) > 20

def mcrterrfm_check(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel sum(count_error) as count_of_error
    from PRD_RP_PRODUCT_VIEW.FNFM_STATUS_WORDS_AGGREGATED_PER_JOB
    where xcol = 'MCRTERRFM' and xcol_decoded in ('FNFM_EIPUplinkMessageSend','FNFM_EIPITCMessageSend', 'FNFM_EIPLoopbackMessageSend', 'FNFM_EIPDownlinkMessageReceive') and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, engine)
    result_value = df.iloc[0, 0]
    return result_value is not None and int(result_value) > 1

def limit_check(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel sum(error_count),min("min"),max("max")
    from PRD_GLBL_DATA_PRODUCTS.FNFM_fleet_timeseries_generic_limit_checks_agg_mavg
    where xcol = '{triple_subject}' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, engine)
    result_value = df.iloc[0, 0]
    return result_value is not None and int(result_value) > 0

def status_check(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel partition_id
    from PRD_GLBL_DATA_PRODUCTS.FNFM_fleet_timeseries_generic_status_checks
    where event_name = '{triple_subject}' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, engine)
    return not df.empty

def large_pump(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel partition_id
    from PRD_GLBL_DATA_PRODUCTS.FNFM_fleet_timeseries_large_pump_cal_check
    where health_indicator = 'Fail' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, engine)
    return not df.empty

def small_pump(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel partition_id
    from PRD_GLBL_DATA_PRODUCTS.FNFM_fleet_timeseries_small_pump_cal_check
    where health_indicator = 'Fail' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, engine)
    return not df.empty

def mterrstafm_check(partition_id, triple_subject):
    engine = get_engine()
    sql = f""" sel sum(count_error) as count_of_error
    from PRD_RP_PRODUCT_VIEW.FNFM_STATUS_WORDS_AGGREGATED_PER_JOB
    where xcol = 'MTERRSTAFM' and xcol_decoded in ('FNFM_FaultIbusFM', 'FNFM_TripPhaseBFM', 'FNFM_TripPhaseCFM', 'FNFM_FaultIbFM', 'FNFM_FaultIaFM', 'FNFM_TripPhaseAFM') and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, engine)
    result_value = df.iloc[0, 0]
    return result_value is not None and int(result_value) > 1

# Mapping function dictionary
MAPPING_FUNCTION = {
    "FNFM Uplink telemetry check": status_check,
    "FNFM LIN device check": status_check,
    "FNFM CAN device check": status_check,
    "FNFM Motor Error Status": mterrstafm_check,
    "FNFM Solenoid PHM HALL Voltage": limit_check,
    "FNFM Solenoid PHM Digital Voltage": limit_check,
    "FNFM Solenoid PHM LIN Voltage ADC": limit_check,
    "FNFM Master Controller Reference Voltage": limit_check,
    "FNFM Master Controller Digital Voltage": limit_check,
    "FNFM Master Controller Input Voltage": limit_check,
    "FNFM Master Controller Core Voltage": limit_check,
    "FNFM Master Controller EIP Core Voltage": limit_check,
    "FNFM Master Controller EIP Digital Voltage": limit_check,
    "FNFM LVPS Digital Voltage": limit_check,
    "FNFM LVPS Positive Analog Voltage": limit_check,
    "FNFM LVPS Negative Analog Voltage": limit_check,
    "FNFM Small pump calibration check": small_pump,
    "FNFM Large pump calibration check": large_pump
}

def execute_function_from_the_map(message, partition_id, datachannel):
    """
    Execution of the function
    """
    if message in MAPPING_FUNCTION:
        return MAPPING_FUNCTION[message](partition_id, datachannel)
    return None
