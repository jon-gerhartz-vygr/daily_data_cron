from etl import snowflake_sync
from queries import *


check_job_data = {
    'extract_query': q_get_bubble_check_updates,
    'tbl_name': 'BUBBLE_CHECK_SYNC_RECORDS',
    'update_query': q_update_bubble_actual_check,
    'log_query': q_write_sync_log
}

user_job_data = {
    'extract_query': q_get_bubble_user_updates,
    'tbl_name': 'BUBBLE_USERS_SYNC_RECORDS',
    'update_query': q_update_bubble_users_actual,
    'log_query': q_write_sync_log
}

jobs = {
    'check': check_job_data,
    'user': user_job_data
}


def run_jobs():
    for job in jobs:
        job_data = jobs[job]
        snowflake_sync(job_data['extract_query'], job, job_data['tbl_name'],
                       job_data['update_query'], job_data['log_query'])
