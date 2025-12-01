import os
import logging
import requests
import pandas as pd
import shutil
from datetime import datetime, timedelta
import time
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

#########################################################
#
#   DAG Settings for Part 1 Question 3
#
#########################################################

dag_default_args = {
    'owner': 'BDE_AT3',
    'start_date': datetime.now() - timedelta(days=1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='part3_data_loading',
    default_args=dag_default_args,
    schedule_interval=None,  
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data/"
CENSUS_DATA = AIRFLOW_DATA + "Census_LGA/"
LISTINGS_DATA = AIRFLOW_DATA + "listings/"
NSW_LGA_DATA = AIRFLOW_DATA + "NSW_LGA/"

#########################################################
#
#   Data Loading Functions
#
#########################################################

def load_census_g01_data(**kwargs):
    """Load 2016Census_G01_NSW_LGA.csv data into BRZ.raw_census_g01 table"""
    
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()
    
    # File path
    file_path = os.path.join(CENSUS_DATA, '2016Census_G01_NSW_LGA.csv')
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None
    
    # Read CSV file
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {len(df)} records from 2016Census_G01_NSW_LGA.csv")
    
    if len(df) > 0:
        # Clear existing data
        cursor.execute("TRUNCATE TABLE BRZ.raw_census_g01;")
        logging.info("Cleared existing data from BRZ.raw_census_g01")
        
        # Prepare data for insertion
        columns = [
            'LGA_CODE_2016', 'Tot_P_M', 'Tot_P_F', 'Tot_P_P', 'Age_0_4_yr_M', 'Age_0_4_yr_F', 'Age_0_4_yr_P',
            'Age_5_14_yr_M', 'Age_5_14_yr_F', 'Age_5_14_yr_P', 'Age_15_19_yr_M', 'Age_15_19_yr_F', 'Age_15_19_yr_P',
            'Age_20_24_yr_M', 'Age_20_24_yr_F', 'Age_20_24_yr_P', 'Age_25_34_yr_M', 'Age_25_34_yr_F', 'Age_25_34_yr_P',
            'Age_35_44_yr_M', 'Age_35_44_yr_F', 'Age_35_44_yr_P', 'Age_45_54_yr_M', 'Age_45_54_yr_F', 'Age_45_54_yr_P',
            'Age_55_64_yr_M', 'Age_55_64_yr_F', 'Age_55_64_yr_P', 'Age_65_74_yr_M', 'Age_65_74_yr_F', 'Age_65_74_yr_P',
            'Age_75_84_yr_M', 'Age_75_84_yr_F', 'Age_75_84_yr_P', 'Age_85ov_M', 'Age_85ov_F', 'Age_85ov_P',
            'Counted_Census_Night_home_M', 'Counted_Census_Night_home_F', 'Counted_Census_Night_home_P',
            'Count_Census_Nt_Ewhere_Aust_M', 'Count_Census_Nt_Ewhere_Aust_F', 'Count_Census_Nt_Ewhere_Aust_P',
            'Indigenous_psns_Aboriginal_M', 'Indigenous_psns_Aboriginal_F', 'Indigenous_psns_Aboriginal_P',
            'Indig_psns_Torres_Strait_Is_M', 'Indig_psns_Torres_Strait_Is_F', 'Indig_psns_Torres_Strait_Is_P',
            'Indig_Bth_Abor_Torres_St_Is_M', 'Indig_Bth_Abor_Torres_St_Is_F', 'Indig_Bth_Abor_Torres_St_Is_P',
            'Indigenous_P_Tot_M', 'Indigenous_P_Tot_F', 'Indigenous_P_Tot_P', 'Birthplace_Australia_M',
            'Birthplace_Australia_F', 'Birthplace_Australia_P', 'Birthplace_Elsewhere_M', 'Birthplace_Elsewhere_F',
            'Birthplace_Elsewhere_P', 'Lang_spoken_home_Eng_only_M', 'Lang_spoken_home_Eng_only_F', 'Lang_spoken_home_Eng_only_P',
            'Lang_spoken_home_Oth_Lang_M', 'Lang_spoken_home_Oth_Lang_F', 'Lang_spoken_home_Oth_Lang_P',
            'Australian_citizen_M', 'Australian_citizen_F', 'Australian_citizen_P', 'Age_psns_att_educ_inst_0_4_M',
            'Age_psns_att_educ_inst_0_4_F', 'Age_psns_att_educ_inst_0_4_P', 'Age_psns_att_educ_inst_5_14_M',
            'Age_psns_att_educ_inst_5_14_F', 'Age_psns_att_educ_inst_5_14_P', 'Age_psns_att_edu_inst_15_19_M',
            'Age_psns_att_edu_inst_15_19_F', 'Age_psns_att_edu_inst_15_19_P', 'Age_psns_att_edu_inst_20_24_M',
            'Age_psns_att_edu_inst_20_24_F', 'Age_psns_att_edu_inst_20_24_P', 'Age_psns_att_edu_inst_25_ov_M',
            'Age_psns_att_edu_inst_25_ov_F', 'Age_psns_att_edu_inst_25_ov_P', 'High_yr_schl_comp_Yr_12_eq_M',
            'High_yr_schl_comp_Yr_12_eq_F', 'High_yr_schl_comp_Yr_12_eq_P', 'High_yr_schl_comp_Yr_11_eq_M',
            'High_yr_schl_comp_Yr_11_eq_F', 'High_yr_schl_comp_Yr_11_eq_P', 'High_yr_schl_comp_Yr_10_eq_M',
            'High_yr_schl_comp_Yr_10_eq_F', 'High_yr_schl_comp_Yr_10_eq_P', 'High_yr_schl_comp_Yr_9_eq_M',
            'High_yr_schl_comp_Yr_9_eq_F', 'High_yr_schl_comp_Yr_9_eq_P', 'High_yr_schl_comp_Yr_8_belw_M',
            'High_yr_schl_comp_Yr_8_belw_F', 'High_yr_schl_comp_Yr_8_belw_P', 'High_yr_schl_comp_D_n_g_sch_M',
            'High_yr_schl_comp_D_n_g_sch_F', 'High_yr_schl_comp_D_n_g_sch_P', 'Count_psns_occ_priv_dwgs_M',
            'Count_psns_occ_priv_dwgs_F', 'Count_psns_occ_priv_dwgs_P', 'Count_Persons_other_dwgs_M',
            'Count_Persons_other_dwgs_F', 'Count_Persons_other_dwgs_P'
        ]
        
        # Get data values
        values = df[columns].to_dict('split')['data']
        
        # Insert data
        insert_sql = f"""
            INSERT INTO BRZ.raw_census_g01({', '.join(columns)})
            VALUES %s
        """
        execute_values(cursor, insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(CENSUS_DATA, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder, exist_ok=True)
        shutil.move(file_path, os.path.join(archive_folder, '2016Census_G01_NSW_LGA.csv'))

    return None


def load_census_g02_data(**kwargs):
    """Load 2016Census_G02_NSW_LGA.csv data into BRZ.raw_census_g02 table"""
    
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()
    
    # File path
    file_path = os.path.join(CENSUS_DATA, '2016Census_G02_NSW_LGA.csv')
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None
    
    # Read CSV file
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {len(df)} records from 2016Census_G02_NSW_LGA.csv")
    
    if len(df) > 0:
        # Clear existing data
        cursor.execute("TRUNCATE TABLE BRZ.raw_census_g02;")
        logging.info("Cleared existing data from BRZ.raw_census_g02")
        
        # Prepare data for insertion
        columns = [
            'LGA_CODE_2016', 'Median_age_persons', 'Median_mortgage_repay_monthly',
            'Median_tot_prsnl_inc_weekly', 'Median_rent_weekly', 'Median_tot_fam_inc_weekly',
            'Average_num_psns_per_bedroom', 'Median_tot_hhd_inc_weekly', 'Average_household_size'
        ]
        
        # Get data values
        values = df[columns].to_dict('split')['data']
        
        # Insert data
        insert_sql = f"""
            INSERT INTO BRZ.raw_census_g02({', '.join(columns)})
            VALUES %s
        """
        execute_values(cursor, insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(CENSUS_DATA, 'archive')
        os.makedirs(archive_folder, exist_ok=True)
        shutil.move(file_path, os.path.join(archive_folder, '2016Census_G02_NSW_LGA.csv'))

    return None


def load_lga_code_data(**kwargs):
    """Load NSW_LGA_CODE.csv data into BRZ.raw_code table"""
    
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()
    
    # File path
    file_path = os.path.join(NSW_LGA_DATA, 'NSW_LGA_CODE.csv')
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None
    
    # Read CSV file
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {len(df)} records from NSW_LGA_CODE.csv")
    
    if len(df) > 0:
        # Clear existing data
        cursor.execute("TRUNCATE TABLE BRZ.raw_code;")
        logging.info("Cleared existing data from BRZ.raw_code")
        
        # Prepare data for insertion
        columns = ['LGA_CODE', 'LGA_NAME']
        
        # Get data values
        values = df[columns].to_dict('split')['data']
        
        # Insert data
        insert_sql = """
            INSERT INTO BRZ.raw_code(LGA_CODE, LGA_NAME)
            VALUES %s
        """
        execute_values(cursor, insert_sql, values, page_size=len(df))
        conn_ps.commit()
        
        # Move the processed file to the archive folder
        archive_folder = os.path.join(NSW_LGA_DATA, 'archive')
        os.makedirs(archive_folder, exist_ok=True)
        shutil.move(file_path, os.path.join(archive_folder, 'NSW_LGA_CODE.csv'))

    return None


def load_lga_suburb_data(**kwargs):
    """Load NSW_LGA_SUBURB.csv data into BRZ.raw_suburb table"""
    
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()
    
    # File path
    file_path = os.path.join(NSW_LGA_DATA, 'NSW_LGA_SUBURB.csv')
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None
    
    # Read CSV file
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {len(df)} records from NSW_LGA_SUBURB.csv")
    
    if len(df) > 0:
        # Clear existing data
        cursor.execute("TRUNCATE TABLE BRZ.raw_suburb;")
        logging.info("Cleared existing data from BRZ.raw_suburb")
        
        # Prepare data for insertion
        columns = ['LGA_NAME', 'SUBURB_NAME']
        
        # Get data values
        values = df[columns].to_dict('split')['data']
        
        # Insert data
        insert_sql = """
            INSERT INTO BRZ.raw_suburb(LGA_NAME, SUBURB_NAME)
            VALUES %s
        """
        execute_values(cursor, insert_sql, values, page_size=len(df))
        conn_ps.commit()
        
        # Move the processed file to the archive folder
        archive_folder = os.path.join(NSW_LGA_DATA, 'archive')
        os.makedirs(archive_folder, exist_ok=True)
        shutil.move(file_path, os.path.join(archive_folder, 'NSW_LGA_SUBURB.csv'))

    return None


def load_airbnb_listings_data(month=None, year=None, **kwargs):
    """Load Airbnb data into BRZ.raw_listings table by month and year"""
    
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()
    
    # File path
    filename = f"{month}_{year}.csv"
    file_path = os.path.join(LISTINGS_DATA, filename)
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None
    
    # Read CSV file
    df = pd.read_csv(file_path)
    
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME', 'HOST_SINCE', 
                    'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD', 'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 
                    'ROOM_TYPE', 'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30', 'NUMBER_OF_REVIEWS',
                    'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS', 
                    'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION', 'REVIEW_SCORES_VALUE']
        
        # Clean price column (remove $ sign and convert to float)
        if 'PRICE' in df.columns:
            df['PRICE'] = df['PRICE'].astype(str).str.replace('$', '').str.replace(',', '')
            df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
        
        # Get data values
        values = df[col_names].to_dict('split')['data']
        
        # Insert data
        insert_sql = f"""
            INSERT INTO BRZ.raw_listings({', '.join(col_names)})
            VALUES %s
        """
        execute_values(cursor, insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(LISTINGS_DATA, 'archive')
        os.makedirs(archive_folder, exist_ok=True)
        shutil.move(file_path, os.path.join(archive_folder, f"{month}_{year}.csv"))
    
    return None

#########################################################
#
#   Function to trigger dbt Cloud Job
#
#########################################################

def trigger_dbt_cloud_job(**kwargs):
    # Get the dbt Cloud URL, account ID, and job ID from Airflow Variables
    dbt_cloud_url = Variable.get("DBT_CLOUD_URL")
    dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
    dbt_cloud_job_id = Variable.get("DBT_CLOUD_JOB_ID")

    # Define the URL for the dbt Cloud job API dynamically using URL, account ID, and job ID
    url = f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}/jobs/{dbt_cloud_job_id}/run/"

    # Get the dbt Cloud API token from Airflow Variables
    dbt_cloud_token = Variable.get("DBT_CLOUD_API_TOKEN")
    print(dbt_cloud_token)

    # Define the headers and body for the request
    headers = {
        'Authorization': f'Token {dbt_cloud_token}',
        'Content-Type': 'application/json'
    }
    data = {
        "cause": "Triggered via API"
    }

    # Make the POST request to trigger the dbt Cloud job
    response = requests.post(url, headers=headers, json=data)

    # Check if the response is successful
    if response.status_code == 200:
        logging.info("Successfully triggered dbt Cloud job.")
        return response.json()
    else:
        logging.error(f"Failed to trigger dbt Cloud job: {response.status_code}, {response.text}")
        raise AirflowException("Failed to trigger dbt Cloud job.")

def sleep_between_tasks(seconds=120, **kwargs):
    """
    Sleep function to allow dbt job to complete before next data load
    """
    time.sleep(seconds)

#########################################################
#
#   DAG Task Definitions
#
#########################################################

# Task 1: Load Census G01 data
load_census_g01_task = PythonOperator(
    task_id='load_census_g01_data',
    python_callable=load_census_g01_data,
    dag=dag
)

# Task 2: Load Census G02 data
load_census_g02_task = PythonOperator(
    task_id='load_census_g02_data',
    python_callable=load_census_g02_data,
    dag=dag
)

# Task 3: Load LGA Code data
load_lga_code_task = PythonOperator(
    task_id='load_lga_code_data',
    python_callable=load_lga_code_data,
    dag=dag
)

# Task 4: Load LGA Suburb data
load_lga_suburb_task = PythonOperator(
    task_id='load_lga_suburb_data',
    python_callable=load_lga_suburb_data,
    dag=dag
)

# Task 5: Load Airbnb listings data by month and year
# Define months in chronological order
months_to_process = [
    ('05', '2020'), ('06', '2020'), ('07', '2020'), ('08', '2020'), 
    ('09', '2020'), ('10', '2020'), ('11', '2020'), ('12', '2020'), 
    ('01', '2021'), ('02', '2021'), ('03', '2021'), ('04', '2021')
]

# Create tasks for each month
all_tasks = []

for i, (month, year) in enumerate(months_to_process):
    
    # Data loading task
    load_task = PythonOperator(
        task_id=f'load_airbnb_{month}_{year}',
        python_callable=load_airbnb_listings_data,
        op_kwargs={'month': month, 'year': year},
        dag=dag
    )
    
    # dbt trigger task
    dbt_task = PythonOperator(
        task_id=f'trigger_dbt_{month}_{year}',
        python_callable=trigger_dbt_cloud_job,
        dag=dag
    )
    
    # Sleep task (except after the last month)
    if i < len(months_to_process) - 1:  
        sleep_task = PythonOperator(
            task_id=f'sleep_after_{month}_{year}',
            python_callable=sleep_between_tasks,
            op_kwargs={'seconds': 120},
            dag=dag
        )
        all_tasks.extend([load_task, dbt_task, sleep_task])
    else:
        all_tasks.extend([load_task, dbt_task])

#########################################################
#
#   Task Dependencies
#
#########################################################

[load_census_g01_task, load_census_g02_task, load_lga_code_task, load_lga_suburb_task] >> all_tasks[0]

for i in range(len(all_tasks) - 1):
    all_tasks[i] >> all_tasks[i + 1]
