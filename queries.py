# bubble check sync
q_get_bubble_check_updates = """
SELECT *
FROM LIQUIDATION_TRUST.SRC.MODIFY_BUBBLE_CHECKS;
"""

q_update_bubble_actual_check = """
UPDATE LIQUIDATION_TRUST.BUBBLE_ACTUAL.BUBBLE_CHECK a
set
    a."issuedTo"=b."issuedTo"
    , a."lastUpdated"=b."lastUpdated"
    , a."payee"=b."payee"
    , a."status"=b."status"
    , a."amount"=b."amount"
    , a.sync_ts='{sync_ts}'
FROM (
    SELECT aa.*
    FROM LIQUIDATION_TRUST.SRC.MODIFY_BUBBLE_CHECKS aa
    JOIN LIQUIDATION_TRUST.STG.BUBBLE_CHECK_SYNC_RECORDS bb on bb."checkNum" = aa."checkNum") b
WHERE a."checkNum" = b."checkNum"
"""

q_get_last_sync = """
SELECT max(sync_ts) as sync_ts
FROM LIQUIDATION_TRUST.SRC.BUBBLE_SYNC_LOG
WHERE sync_status = 'complete'
"""

q_write_sync_log = """
INSERT INTO LIQUIDATION_TRUST.SRC.BUBBLE_SYNC_LOG (sync_ts, initiated_from, sync_status, data_type)
VALUES ('{sync_ts}', 'streamlit', '{sync_status}', '{data_type}');
"""

# bubble user sync
q_get_bubble_user_updates = """
SELECT 
    "unique id"
    , "address1"
    , "address2"
    , "city"
    , "fullName"
    , "state"
    , "zip"
    , "vygrUserId"
FROM LIQUIDATION_TRUST.SRC.MODIFY_BUBBLE_USERS
"""

q_update_bubble_users_actual = """
UPDATE LIQUIDATION_TRUST.BUBBLE_ACTUAL.BUBBLE_USERS a
set
    a."address1"=b."address1"
    , a."address2"=b."address2"
    , a."city"=b."city"
    , a."fullName"=b."fullName"
    , a."state"=b."state"
    , a."zip"=b."zip"
    , a.sync_ts='{sync_ts}'
FROM (
    SELECT aa.*
    FROM LIQUIDATION_TRUST.SRC.MODIFY_BUBBLE_USERS aa
    JOIN LIQUIDATION_TRUST.STG.BUBBLE_USERS_SYNC_RECORDS bb on bb."vygrUserId" = aa."vygrUserId") b
WHERE a."vygrUserId" = b."vygrUserId"
"""
