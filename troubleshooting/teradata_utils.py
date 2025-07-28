import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    user = os.getenv("TERADATA_USER")
    pasw = os.getenv("TERADATA_PASS")
    host = os.getenv("TERADATA_HOST")
    port = os.getenv("TERADATA_PORT")
    return create_engine(f'teradatasql://{user}:{pasw}@{host}/?encryptdata=true')

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

def run_checks(partition_id, datachannel, check_type):
    engine = get_engine()
    # Example: implement your check logic here
    # You can expand this with your actual SQLs and logic
    if check_type == "status_check":
        sql = f"""sel partition_id
        from PRD_GLBL_DATA_PRODUCTS.FNFM_fleet_timeseries_generic_status_checks
        where event_name = '{datachannel}' and partition_id= '{partition_id}' """
        df = pd.read_sql(sql, engine)
        return not df.empty
    # Add more check types as needed
    return None