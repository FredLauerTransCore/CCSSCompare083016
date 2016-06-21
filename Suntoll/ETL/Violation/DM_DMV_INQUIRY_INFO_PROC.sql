/********************************************************
*
* Name: DM_DMV_INQUIRY_INFO_PROC
* Created by: RH, 5/19/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_DMV_INQUIRY_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_DMV_INQUIRY_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_DMV_INQUIRY_INFO_TYP IS TABLE OF DM_DMV_INQUIRY_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_DMV_INQUIRY_INFO_tab DM_DMV_INQUIRY_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    0 MODIFICATION_NUM --number of times the account or plate informations were modified.
    ,'0' CONFLICT_ID
    ,e.ACCT_NUM X_ETC_ACCOUNT_ID
    ,'0' X_HIST_FLG
    ,ev.ACCT_NUM X_ACCOUNT_ID  -- JOIN TO EVENT_ADDRESS TABLE TO GAIN ACCOUNT_id
    ,ea.ADDR1 X_ADDR_LINE_1
    ,ea.ADDR2 X_ADDR_LINE_2
    ,ea.CITY X_CITY
    ,ea.ID X_CONTACT_ID
    ,ea.COUNTRY_CODE X_COUNTRY

-- IF REG_STOP_SENT_ON IS NOT NULL AND REG_REMOVAL_SENT_ON IS NULL THEN  REG_STOP ELSE 'N' 
    ,CASE WHEN srs.REG_STOP_SENT_ON IS NOT NULL AND srs.REG_STOP_REMOVAL_SENT_ON IS NULL
          THEN srs.REG_STOP  --srs.REG_STOP // Further action required per comment 
          ELSE 'N'
      END X_DMV_STATUS
      
    ,eo.FIRST_NAME X_FST_NAME
    ,eo.LAST_ORG_NAME X_LAST_NAME
    ,ev.PLATE X_PLATE_NUM
    ,CASE WHEN REG_STOP_SENT_ON IS NOT NULL 
          THEN 'Y' 
          ELSE 'N'
      END X_SEND_DMV  -- IF REG_STOP_SENT_ON IS NOT NULL THEN 'Y' ELSE 'N'
    ,ea.STATE_ABBR X_STATE
    ,'00000' X_STATUS
    ,NULL X_TOD_ID
    ,ea.ZIP X_ZIPCODE
    ,e.CREATED_ON CREATED -- e.OWNER_ID CREATED
    ,e.OWNER_ID CREATED_BY  -- e.CREATED_ON CREATED_BY
    ,SYSDATE LAST_UPD
    ,'CURRENT' LAST_UPD_BY
--    ,'SUNTOLL' SOURCE_SYSTEM
FROM EVENT_OWNER_ADDR_VEHICLE_ACCT e
    ,EVENT_ADDRESS ea
    ,EVENT_VEHICLE ev
    ,EVENT_OWNER eo
    ,ST_REG_STOP srs  
--    ,PA_LANE_TXN lt
WHERE e.OWNER_ID = eo.ID (+)
AND e.ADDRESS_ID = ea.ID (+)
AND e.VEHICLE_ID = ev.ID (+)
AND e.ACCT_NUM = srs.ACCT_NUM (+)
--AND   e.ACCT_NUM >= p_begin_acct_num AND   e.ACCT_NUM <= p_end_acct_num
; 
-- Source

-- EXTRACT RULE - 
--select * 
--Join OAVA_LINK_ID of PA_LANE_TXN to ID of EVENT_OWNER_ADDR_VEHICLE_ACCT 
--AND OWNER_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_OWNER 
--AND ADDRESS_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_ADDRESS 
--AND VEHICLE_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_VEHICLE

row_cnt          NUMBER := 0;
v_trac_rec       dm_tracking%ROWTYPE;
v_trac_etl_rec   dm_tracking_etl%ROWTYPE;

BEGIN
  SELECT * INTO v_trac_etl_rec
  FROM  dm_tracking_etl
  WHERE track_etl_id = i_trac_id;
  DBMS_OUTPUT.PUT_LINE('Start '||v_trac_etl_rec.etl_name||' ETL load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  v_trac_etl_rec.status := 'ETL Start ';
  v_trac_etl_rec.proc_start_date := SYSDATE;  
  update_track_proc(v_trac_etl_rec);
 
  SELECT * INTO   v_trac_rec
  FROM   dm_tracking
  WHERE  track_id = v_trac_etl_rec.track_id
  ;

  OPEN C1;  -- (v_trac_rec.begin_acct,v_end_acct,end_acct);  
--  v_trac_etl_rec.begin_val := v_trac_rec.begin_acct;
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_DMV_INQUIRY_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_DMV_INQUIRY_INFO_tab.first .. DM_DMV_INQUIRY_INFO_tab.last
           INSERT INTO DM_DMV_INQUIRY_INFO VALUES DM_DMV_INQUIRY_INFO_tab(i);

    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total ROW_CNT : '||ROW_CNT);

  COMMIT;

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  
  EXCEPTION
  WHEN OTHERS THEN
    v_trac_etl_rec.result_code := SQLCODE;
    v_trac_etl_rec.result_msg := SQLERRM;
    v_trac_etl_rec.proc_end_date := SYSDATE;
    update_track_proc(v_trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS


