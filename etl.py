from datetime import datetime
from dotenv import load_dotenv
import os
from utils import fetch_pandas, update_bubble_thing, update_bubble_email, execute_query, load_data, update_bubble_check
from queries import *


load_dotenv()
BUBBLE_KEY = os.getenv("BUBBLE_KEY")


def log_success(success_responses, tbl_name):
    try:
        success_df = pd.DataFrame.from_records(success_responses)
        success = load_data(success_df, tbl_name, 'STG')
        message = 'logged records to db'
        assert success, 'failed to load data to success table'

    except Exception as e:
        print(e)
        message = 'failed to log to db'

    finally:
        return message


def begin(obj, get_updates_query):
    print(f'starting {obj} sync')
    updates_df = fetch_pandas(get_updates_query)
    updates_count = len(updates_df.index)
    print(f'Found {updates_count} records to sync')
    updates = updates_df.to_dict(orient='records')
    return updates, updates_count


def run_sync(updates, obj, tbl_name, n=250):
    success_responses = []
    fail_responses = []
    success_count = 0
    print('Starting sync...')

    try:
        index = 0
        for i in updates:
            obj_id = i.pop('unique id')
            if obj == 'user':
                email = i.pop('email')
                user_id = i['vygrUserId']
                email_resp = update_bubble_email(user_id, email, BUBBLE_KEY)
            if obj == 'check':
                i['lastUpdated'] = str(i['lastUpdated'])
                resp = update_bubble_check(i, BUBBLE_KEY)
            else:
                resp = update_bubble_thing(obj, obj_id, i, BUBBLE_KEY)

            i['unique id'] = obj_id

            if resp.ok:
                success_responses.append(i)
            else:
                i['error_message'] = resp.text
                fail_responses.append(i)
            if (index+1) % n == 0:
                log_resp = log_success(success_responses, tbl_name)
                success_count += len(success_responses)
                success_responses.clear()
            index += 1

        if len(success_responses) > 0:
            log_resp = log_success(success_responses, tbl_name)
            success_count += len(success_responses)

        failure_count = len(fail_responses)
        total_count = success_count + failure_count
        success_rate = round((success_count/total_count), 1) * 100
        if failure_count > 0:
            print(fail_responses)

        print(f'Bubble updates complete, success rate: {success_rate}')
        return 'complete'
    except Exception as e:
        print(e.args[0])
        return 'failed'


def update_bubble_actual(update_bubble_actual_query):
    sync_dt = datetime.now()
    sync_ts = sync_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    print('updating snowflake tables...')
    try:
        update_bubble_actual_query_formatted = update_bubble_actual_query.format(
            sync_ts=sync_ts)
        resp = execute_query(update_bubble_actual_query_formatted)
        status = 'complete'
    except:
        status = 'failed'

    return sync_ts, status


def write_sync_log(write_sync_log_query, sync_ts, sync_status, obj):
    print('logging sync status...')
    try:
        write_sync_log_query_formatted = write_sync_log_query.format(
            sync_ts=sync_ts, sync_status=sync_status, data_type=obj.upper())
        resp = execute_query(write_sync_log_query_formatted)
        return resp
    except Exception as e:
        print(e)


def snowflake_sync(get_updates_query, obj, tbl_name, update_bubble_actual_query, write_sync_log_query):
    updates_df, updates_count = begin(obj, get_updates_query)
    if updates_count > 0:
        sync_status = run_sync(updates_df, obj, tbl_name)
        sync_ts, update_status = update_bubble_actual(
            update_bubble_actual_query)
        status = f"sync status: {sync_status}, update_status: {update_status}"
        write_sync_log(write_sync_log_query, sync_ts, status, obj)
    else:
        status = "No records found to sync, ending job now."
    print(status)
    return status
