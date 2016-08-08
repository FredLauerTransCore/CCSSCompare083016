/********************************************************
*
* Name: DM_TOLL_EXCEPTIONS_IMG_IN_PROC
* Created by: RH, 5/22/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_TOLL_EXCEPTIONS_IMG_IN
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/21/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_TOLL_EXCEPTIONS_IMG_IN_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_TOLL_EXCEPTIONS_IMG_IN_TYP IS TABLE OF DM_TOLL_EXCEPTIONS_IMG_IN%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_TOLL_EXCEPTIONS_IMG_IN_tab DM_TOLL_EXCEPTIONS_IMG_IN_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    TXN_ID TX_EXTERN_REF_NO
    ,EXT_MSG_SEQ_NUM TX_SEQ_NUMBER
    ,0 EXTERN_FILE_ID
    ,EXT_LANE_ID LANE_ID  -- Xerox - internal lookup for the internal lane ID
    ,EXT_DATE_TIME TX_TIMESTAMP
    ,TXN_ID LANE_TXN_ID
    ,NVL2(TRANS_SOURCE,1,0) TX_MOD_SEQ  -- Derived IF TRANS_SOURCE IS NULL THEN 0 ELSE 1
    ,'F' TX_TYPE_IND
    ,'Z' TX_SUBTYPE_IND
    ,DECODE(substr(EXT_PLAZA_ID,1,3),'004', 'C', 'B') TOLL_SYSTEM_TYPE  -- Derived IF PLAZA_ID STARTS WITH '004' THEN 'C' ELSE 'B'
    ,EXT_LANE_TYPE_CODE LANE_MODE
    ,1 LANE_TYPE
    ,0 LANE_STATE
    ,0 LANE_HEALTH
    ,(select AGENCY_ID from PATRON.ST_INTEROP_AGENCIES where ENT_PLAZA_ID = lt.EXT_PLAZA_ID)
        PLAZA_AGENCY_ID 
    ,EXT_PLAZA_ID PLAZA_ID  -- Xerox - internal lookup for internal Plaza ID
    ,0 COLLECTOR_ID
    ,0 TOUR_SEGMENT_ID
    ,0 ENTRY_DATA_SOURCE
    ,0 ENTRY_LANE_ID
    ,0 ENTRY_PLAZA_ID
    ,0 ENTRY_TIMESTAMP
    ,0 ENTRY_TX_SEQ_NUMBER
    ,0 ENTRY_VEHICLE_SPEED
    ,0 LANE_TX_STATUS
    ,0 LANE_TX_TYPE
    ,0 TOLL_REVENUE_TYPE
    ,0 ACTUAL_CLASS
    ,0 ACTUAL_AXLES
    ,0 ACTUAL_EXTRA_AXLES
    ,0 COLLECTOR_CLASS
    ,0 COLLECTOR_AXLES
    ,0 PRECLASS_CLASS
    ,0 PRECLASS_AXLES
    ,0 POSTCLASS_CLASS
    ,0 POSTCLASS_AXLES
    ,0 FORWARD_AXLES
    ,0 REVERSE_AXLES
    ,0 FULL_FARE_AMOUNT
    ,0 DISCOUNTED_AMOUNT
    ,0 COLLECTED_AMOUNT
    ,0 UNREALIZED_AMOUNT
    ,0 IS_DISCOUNTABLE
    ,0 IS_MEDIAN_FARE
    ,0 IS_PEAK
    ,0 PRICE_SCHEDULE_ID
    ,0 VEHICLE_SPEED
    ,0 RECEIPT_ISSUED
    ,0 DEVICE_NO
    ,0 ACCOUNT_TYPE
    ,0 DEVICE_CODED_CLASS
    ,0 DEVICE_AGENCY_CLASS
    ,0 DEVICE_IAG_CLASS
    ,0 DEVICE_AXLES
    ,0 ETC_ACCOUNT_ID
    ,0 ACCOUNT_AGENCY_ID
    ,0 READ_AVI_CLASS
    ,0 READ_AVI_AXLES
    ,0 DEVICE_PROGRAM_STATUS
    ,0 BUFFERED_READ_FLAG
    ,0 LANE_DEVICE_STATUS
    ,0 POST_DEVICE_STATUS
    ,0 PRE_TXN_BALANCE
    ,0 PLAN_TYPE_ID
    ,0 ETC_TX_STATUS
    ,0 SPEED_VIOL_FLAG
    ,0 IMAGE_TAKEN
    ,0 PLATE_COUNTRY
    ,0 PLATE_STATE
    ,0 PLATE_NUMBER
    ,0 REVENUE_DATE  -- date?
    ,0 POSTED_DATE  -- date?
    ,0 ATP_FILE_ID
    ,0 UPDATE_TS
    ,0 IS_REVERSED
    ,0 CORR_REASON_ID
    ,0 RECON_DATE  -- date?
    ,0 RECON_STATUS_IND
    ,0 RECON_SUB_CODE_IND
    ,0 EXTERN_FILE_DATE  -- date?
    ,0 MILEAGE
    ,0 DEVICE_READ_COUNT
    ,0 DEVICE_WRITE_COUNT
    ,0 ENTRY_DEVICE_READ_COUNT
    ,0 ENTRY_DEVICE_WRITE_COUNT
    ,0 DEPOSIT_ID
    ,0 TX_DATE  -- date?
    ,0 LOCATION
    ,0 FARE_TBL_ID
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_LANE_TXN_REJECT
--AND   paa.ACCT_NUM >= p_begin_acct_num AND   paa.ACCT_NUM <= p_end_acct_num
; -- Source  -- FTE TO SEND PA_LANE_TXN_REJECT TABLE

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

  OPEN C1;   -- (v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_TOLL_EXCEPTIONS_IMG_IN_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_TOLL_EXCEPTIONS_IMG_IN_tab.first .. DM_TOLL_EXCEPTIONS_IMG_IN_tab.last
           INSERT INTO DM_TOLL_EXCEPTIONS_IMG_IN VALUES DM_TOLL_EXCEPTIONS_IMG_IN_tab(i);
                       
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



