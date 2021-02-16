#
# Author        	: WhereScape
# Date          	: 2020-11-11
# Version       	: 2.0
#
# Description
# ===========
# This Airflow DAG is used to execute a WhereScape job
# Version 2.0 includes automatic rerelease of Jobs that complete successfully with warnings.
from airflow import DAG
from airflow.contrib.sensors.wasb_sensor import WasbBlobSensor,WasbPrefixSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from time import sleep
import logging
import pyodbc

# -------------------------------------------------------
# WhereScape job-specific parameters
# -------------------------------------------------------
ws_jobName = "JOB_ANA_TPM_BS2_FILE_COMPLIANCE_FLG"        	# --> Set WhereScape Job name
dag_name = "Temp_WS_AU_" + ws_jobName           			# --> Set name of the DAG
start_date = datetime.strptime("2020-07-07 01:30:39.000",'%Y-%m-%d %H:%M:%S.%f')   			        # --> Set DAG start date
schedule_interval = "@daily"               			# --> Set DAG schedule interval
dag_max_active_runs = 1                 			# --> Keep 1 as WhereScape manages parallelism                                     																					# --> Keep 1 as WhereScape manages parallelism

# -------------------------------------------------------
# WhereScape enablement functions
# -------------------------------------------------------

def ms_connect():
	# Defines and returns the pyodbc connection string
		# Some other example server values are
		# server = 'localhost\sqlexpress' # for a named instance
		# server = 'myserver,port' # to specify an alternate port
		# server = 'tcp:myserver.database.windows.net'
    driver = "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.7.so.1.1"
    server   = Variable.get("ws_server")                     			# server = Variable.get("ws_server")   				ws_server should be set as a variable in Airflow
    port     = Variable.get("ws_port")                       			# port = Variable.get("ws_port")      					ws_port should be set as a variable in Airflow Insert port for SQL Server Repo DB here"
    database = Variable.get("ws_aus_db")                   	# database = Variable.get("ws_database")      	ws_database should be set as a variable in Airflow
    username = Variable.get("ws_username")                   	# username = Variable.get("ws_username")  	ws_username should be set as a variable in Airflow
    password = Variable.get("ws_password")                   	# password = Variable.get("ws_password")  	ws_password should be set as a variable in Airflow
    conn = pyodbc.connect('DRIVER={'+driver+'};SERVER='+server+','+port+';DATABASE='+database+';UID='+username+';PWD='+password)
    return conn


def exec_ws_sql(conn, sql_stmt):
	# Defines framework used to execute SQL Statements 
    logging.debug("SQL statement:")
    logging.debug(sql_stmt)
    cursor = conn.cursor()
    cursor.execute(sql_stmt)
    row = cursor.fetchone()
    conn.commit()
    logging.debug("Return code: " + row[0])
    logging.debug("Return message: " + row[1])
    logging.debug("Result number: " + str(row[2]))
    logging.debug("Simplified job status code: " + str(row[3]))
    logging.debug("Standard job status code: " + str(row[4]))
    logging.debug("Enhanced job status number: " + str(row[5]))
    if row[2] < 1:
        raise ValueError(row[1])
    return row


def job_create(jobName, conn):
    sql_template = Variable.get("ws_job_release")
    job_create_sql = sql_template.replace("@JobName", jobName)
    result = exec_ws_sql(conn, job_create_sql)


def job_restart(jobName, conn):
    sql_template = Variable.get("ws_job_restart")
    job_restart_sql = sql_template.replace("@JobName", jobName)
    result = exec_ws_sql(conn, job_restart_sql)


def job_status(jobName, conn):
    sql_template = Variable.get("ws_job_status")
    job_status_sql = sql_template.replace("@JobName", jobName)
    result = exec_ws_sql(conn, job_status_sql)
    simpleStatus = result[3]
    return simpleStatus


def job_warning(jobName, conn):
    sql_template = Variable.get("ws_job_status")
    job_status_sql = sql_template.replace("@JobName", jobName)
    result = exec_ws_sql(conn, job_status_sql)
    warningStatus = (result[3],result[5])
    return warningStatus


def job_wait(jobName, conn):
    max_start_checks = 3
    sleep_interval = 30     # Waiting time in seconds
    start_check_cnt = 0
    while True:
        status = job_status(jobName, conn)
        logging.info("Job status = " + status)
        if status == "N" or status == "0":
            start_check_cnt += 1
            if start_check_cnt > max_start_checks:
                logging.error("Job did not start after " +
                              str(max_start_checks) +
                              " iteration checks")
                return "F"
        elif status == "F":
            logging.error('Job failed!')
            return "F"
        elif status == "C":
            logging.info("Job completed successfully")
            return "C"
        elif status == "R":
            logging.info("Sleeping for " + str(sleep_interval) + " seconds...")
        else:
            raise ValueError("Unknown status!")
        sleep(sleep_interval)


def job_wait_rerelease(jobName, conn):
    max_start_checks = 3
    sleep_interval = 30     # Waiting time in seconds
    start_check_cnt = 0
    while True:
        status = job_warning(jobName, conn)
        logging.info("Job status = " + status[0] + status[1])
        if status[0] == "N" or status[0] == "0":
            start_check_cnt += 1
            if start_check_cnt > max_start_checks:
                logging.error("Job did not start after " +
                              str(max_start_checks) +
                              " iteration checks")
                return "F"
        elif status[0] == "F":
            logging.error('Job rerelease failed!')
            return "F"
        elif status[1] == "10":
            logging.error('Job completed with warnings!')
            return "F"
        elif status[0] == "C":
            logging.info("Job completed successfully")
            return "C"
        elif status[0] == "R":
            logging.info("Sleeping for " + str(sleep_interval) + " seconds...")
        else:
            raise ValueError("Unknown status!")
        sleep(sleep_interval)


def WhereScapeJob_Release(**kwargs):
    jobName = ws_jobName
    logging.info("Job Name = " + ws_jobName)
    # mssql = MsSqlHook(mssql_conn_id=ws_connectionId)
    # with mssql.get_conn() as conn:
    with ms_connect() as conn:
        # Check if job is already running before releasing it
        status = job_status(jobName, conn)
        logging.info("Status = " + status)
        if status == "R":
            raise ValueError("Job " + runJob +
                             " is already running with status " +
                             status)
        job_create(jobName, conn)
        logging.info("Job released")


def WhereScapeJob_Wait(**kwargs):
    jobName = ws_jobName
    logging.info("Checking status of WhereScape job: " + jobName)
    # mssql = MsSqlHook(mssql_conn_id=ws_connectionId)
    # with mssql.get_conn() as conn:
    with ms_connect() as conn:
        status = job_wait(jobName, conn)
        if status == "F":
            raise ValueError("Job failed")
        else:
            logging.info("Job completed successfully")


def WhereScapeJob_Restart(*args, **kwargs):
    jobName = ws_jobName
    logging.info("Checking status of WhereScape job: " + jobName)
    # mssql = MsSqlHook(mssql_conn_id=ws_connectionId)
    # with mssql.get_conn() as conn:
    with ms_connect() as conn:
        # Call restart procedure
        job_restart(jobName, conn)
        # Wait for execution
        status = job_wait(jobName, conn)
        if status == "F":
            raise ValueError("Job failed")
        else:
            logging.info("Job completed successfully")


def WhereScapeJob_Results(**kwargs):
    jobName = ws_jobName
    sql_template = Variable.get("ws_task_status")
    sql_stmt = sql_template.replace("@JobName", jobName)
    with ms_connect() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_stmt)
        for row in cursor.fetchall():
            logging.info(row[0])
        conn.commit()

def WhereScapeJob_Validate(**kwargs):
    jobName = ws_jobName
    with ms_connect() as conn:
        # Wait for execution
        status = job_warning(jobName, conn)
        if status[1] == "10":
            logging.info("Job failed due warnings")
            job_create(jobName, conn)
            logging.info("Job released for restart")            
        else:
            logging.info("Job completed successfully")

def WhereScapeJob_ValidateWait(**kwargs):
    jobName = ws_jobName
    logging.info("Checking status of WhereScape job: " + jobName)
    with ms_connect() as conn:
        status = job_wait_rerelease(jobName, conn)
        if status == "F":
            raise ValueError("Job failed")
        else:
            logging.info("Job completed successfully")


# -------------------------------------------------------
# Airflow default DAG parameters
# -------------------------------------------------------


dag_default_args = {
    "owner": "Australia",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": start_date,
    "retries": 0,
    "retry_delay": timedelta(seconds=30)
}
 

dag = DAG(dag_name,
          default_args=dag_default_args,
          start_date=start_date,
          schedule_interval=schedule_interval,
		  catchup=False,
          max_active_runs=dag_max_active_runs)


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------

    #waiting_for_tweets = FileSensor(task_id="waiting_for_tweets",fs_conn_id="fs_tweet", filepath="data.csv", poke_interval=5)


#check_connection_opr = PythonOperator(task_id=’connection’,
# python_callable=check_connection)
# file_upload_opr = PythonOperator(task_id=’file_uploader’,
# python_callable=file_upload)


check_prefix = WasbPrefixSensor(
    task_id='sensor_blob_prefix', 
    container_name='anaplan',
    prefix='Landing/BAT_TPM_BS2_FILE_COMPLIANCE_FLG',
    wasb_conn_id='AUS_RAW_BLOB',
    poke_interval=int(Variable.get("AU_FF_POKE_INTERVAL")),
    timeout=int(Variable.get("AU_FF_TIMEOUT")),
    dag=dag)
   


validate_prefix = BashOperator(
    task_id='check',
    bash_command='echo "{{ params.message }}"',
    params={'message': 'Blob Objects, Found!'},
    dag=dag)
    
start_task = DummyOperator(task_id="Start",
                           dag=dag)

release_job = PythonOperator(task_id="ReleaseWhereScapeJob",
                             provide_context=True,
                             python_callable=WhereScapeJob_Release,
                             dag=dag)

wait_job = PythonOperator(task_id='WaitForWhereScapeJob',
                          python_callable=WhereScapeJob_Wait,
                          on_retry_callback=WhereScapeJob_Restart,
                          retries=3,
                          retry_delay=timedelta(minutes=5),
                          dag=dag)

log_results = PythonOperator(task_id='LogResults',
                             python_callable=WhereScapeJob_Results,
                             dag=dag)

validate_job = PythonOperator(task_id='ValidateJob',
                             provide_context=True,
                             python_callable=WhereScapeJob_Validate,
                             dag=dag)                            

validate_wait_job = PythonOperator(task_id='WaitForWhereScapeValidateJob',
                          python_callable=WhereScapeJob_ValidateWait,
                          on_retry_callback=WhereScapeJob_Validate,
                          retries=3,
                          retry_delay=timedelta(minutes=5),
                          dag=dag)

validate_log_results = PythonOperator(task_id='ValidateLogResults',
                             python_callable=WhereScapeJob_Results,
                             dag=dag)




end_task = DummyOperator(task_id="End", dag=dag)

# Execution flow
check_prefix \
    >> validate_prefix \
    >> start_task \
    >> release_job \
    >> wait_job \
    >> log_results \
    >> validate_job \
    >> validate_wait_job \
    >> validate_log_results \
    >> end_task