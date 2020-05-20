# keep all these import, you don't need to install any of them
from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
import pendulum


args = {
    # replace with your OD username
    "owner": "sburman4",
    # replace with your server username (should be the same with your OD username)
    "run_as_user": "sburman4",
    # your job usually should not depend on past
    "depends_on_past": False,
    # if you schedule daily/weekly/monthly job, this will be your start date
    # if you set hourly, minutely, etc. This date doesn't matter
    "start_date": datetime(2019,8,2),
    # your email address list for notification
    "email": ['sburman4@apple.com'],
    # whether to send email to you when job failed (default=True)
    "email_on_failure": True,
    # whether to send email to you when job retired (default=False)
    "email_on_retry": False,
    # How many times you want job to retry before failure
    "retries": 3,
    # Time interval before job retries
    "retry_delay": timedelta(minutes=1),
    # job queue choices: dev, test, prod
    "queue": "prod",
    # 1 to 10, 10 is highest priority
    "priority_weight": 10,

}

my_workflow_dag= DAG(
    dag_id='Helix_Music_Label_Report_without_COB_check',
    description='Helix_Music_Label_Report_without_COB_check',
    default_args=args,
    catchup=False, # please keep this as False
    schedule_interval='0 19 * * 5', #'0 17 * * 5',  #cron style interval in *UTC* '0 17 * * 5'
    dagrun_timeout=timedelta(hours=14) # if you job doesn't finish within this time, mark it as failed
)

task_0 = BashOperator(
    task_id='setup_environment',
    bash_command="""
        cd /home/sburman4/MR_Jobs/Helix_Music_Label_Report_without_COB_check/logs/env
        echo $(date);
        rm -r *
        echo $(date +%F_%H-%M-%S) >> run_date_time.txt
        echo 'Completed!';
    """,
    dag=my_workflow_dag
)


task_1 = BashOperator(
    task_id='create_tables',
    bash_command="""
        cd /home/sburman4/MR_Jobs/Helix_Music_Label_Report_without_COB_check/
        source venv_som/bin/activate
        echo $(date);
        run_date_time=`cat /home/sburman4/MR_Jobs/Helix_Music_Label_Report_without_COB_check/logs/env/run_date_time.txt`
        pip install pandas
        pip install requests
        pip install openpyxl
        pip install pytz
        python runBatch.py "insert_date_intermediate_us_ww.txt" "" "arg_file.txt" "" "insert" >> /home/sburman4/MR_Jobs/Helix_Music_Label_Report_without_COB_check/logs/helix_report_without_cob_task1_$run_date_time.log
        echo 'Completed!';
    """,
    dag=my_workflow_dag
)

task_2 = BashOperator(
    task_id='run_all_queries',
    bash_command="""
        set -e
        cd /home/sburman4/MR_Jobs/Helix_Music_Label_Report_without_COB_check/
        source venv_som/bin/activate
        echo $(date);
        run_date_time=`cat /home/sburman4/MR_Jobs/Helix_Music_Label_Report_without_COB_check/logs/env/run_date_time.txt`
        pip install pandas
        pip install requests
        pip install openpyxl
        pip install pytz
        python runBatch.py "helix_input.txt" "comments.txt" "arg_file.txt" "Helix_Music_Label_Report_without_COB_check.xlsx" "select" >> /home/sburman4/MR_Jobs/Helix_Music_Label_Report_without_COB_check/logs/helix_report_without_cob_task2_$run_date_time.log
        echo 'Completed!';
    """,
    dag=my_workflow_dag
)

task_3 = BashOperator(
    task_id='send_email',
    bash_command="""
        cd /home/sburman4/MR_Jobs/Helix_Music_Label_Report_without_COB_check/
        echo 'Sending Helix Report email'
        sh send_email.sh
    """,
    dag=my_workflow_dag
)



"""Set dependency among tasks
a >> b <=> a.set_downstream(b) # means b requires a to finish first
a << b <=> a.set_upstream(b)   # means a requires b to finish first
"""
task_0 >> task_1  >> task_2 >> task_3
