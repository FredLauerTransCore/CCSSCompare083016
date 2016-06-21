/********************************************************
*
* Name: DM_ACCOUNT_TOLL_INFO_PROC
* Created by: RH, 5/20/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCOUNT_TOLL_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_ACCOUNT_TOLL_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_ACCOUNT_TOLL_INFO_TYP IS TABLE OF DM_ACCOUNT_TOLL_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_TOLL_INFO_tab DM_ACCOUNT_TOLL_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

-- EXTRACT RULE = SELECT * FROM PA_LANE_TXN WHERE TRANSPONDER_ID ENDS WITH '2010'  
/* indicates a violation toll */ -- (rentals included) 

CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
--    UFM_LANE_TXN_INFO.HOST_UFM_TOKEN TX_EXTERN_REF_NO -- JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN
    (select HOST_UFM_TOKEN from UFM_LANE_TXN_INFO where TXN_ID = lt.TXN_ID)  
        TX_EXTERN_REF_NO
    ,lt.EXT_MSG_SEQ_NUM TX_SEQ_NUMBER
    ,lt.TOUR_TOUR_SEQ EXTERN_FILE_ID
    ,NVL(lt.EXT_LANE_ID,1) LANE_ID
    ,lt.EXT_DATE_TIME TX_TIMESTAMP
    ,CASE WHEN lt.MSG_ID IN ('TCA','XADJ','FADJ') 
          THEN 1 ELSE 0
      END TX_MOD_SEQ   -- IF MSG_ID IN ('TCA','XADJ','FADJ') THEN 1 ELSE 0
    ,'V' TX_TYPE_IND
    ,'I' TX_SUBTYPE_IND
    ,DECODE(substr(pp.PLAZA_ID,1,3),'004', 'C', 'B')
        TOLL_SYSTEM_TYPE -- IF PLAZA_ID STARTS WITH '004' THEN 'C' ELSE 'B'
    ,lt.EXT_LANE_TYPE_CODE LANE_MODE
    ,1 LANE_TYPE
    ,0 LANE_STATE
    ,0 LANE_HEALTH
--    AGENCY_ID.PLAZA_AGENCY_ID -- Join ST_INTEROP_AGENCIES and PA_PLAZA on ENT_PLAZA_ID to PLAZA_ID
    ,(select AGENCY_ID from ST_INTEROP_AGENCIES where ENT_PLAZA_ID = pp.PLAZA_ID)  
        PLAZA_AGENCY_ID 
    ,lt.EXT_PLAZA_ID PLAZA_ID
    ,0 COLLECTOR_ID
    ,0 TOUR_SEGMENT_ID
    ,0 ENTRY_DATA_SOURCE
    ,lt.ENT_LANE_ID ENTRY_LANE_ID
    ,lt.ENT_PLAZA_ID ENTRY_PLAZA_ID
    ,lt.ENT_DATE_TIME ENTRY_TIMESTAMP
    ,lt.TXN_ENTRY_NUMBER ENTRY_TX_SEQ_NUMBER
    ,0 ENTRY_VEHICLE_SPEED
    ,0 LANE_TX_STATUS
    ,0 LANE_TX_TYPE
    ,0 TOLL_REVENUE_TYPE
    ,lt.AVC_CLASS ACTUAL_CLASS
    ,lt.AVC_CLASS ACTUAL_AXLES
    ,0 ACTUAL_EXTRA_AXLES
    ,0 COLLECTOR_CLASS
    ,0 COLLECTOR_AXLES
    ,0 PRECLASS_CLASS
    ,0 PRECLASS_AXLES
    ,0 POSTCLASS_CLASS
    ,0 POSTCLASS_AXLES
    ,0 FORWARD_AXLES
    ,0 REVERSE_AXLES
    ,lt.TOLL_AMT_FULL FULL_FARE_AMOUNT
    ,lt.TOLL_AMT_CHARGED DISCOUNTED_AMOUNT
    ,lt.TOLL_AMT_COLLECTED COLLECTED_AMOUNT
    ,0 UNREALIZED_AMOUNT
    ,'N' IS_DISCOUNTABLE
    ,'N' IS_MEDIAN_FARE
    ,'N' IS_PEAK
    ,0 PRICE_SCHEDULE_ID
    ,0 VEHICLE_SPEED
    ,0 RECEIPT_ISSUED
    ,lt.TRANSP_ID DEVICE_NO
    ,(select pa.ACCTTYPE_ACCT_TYPE_CODE from PA_ACCT pa where pa.ACCT_NUM=kl.ACCT_NUM)
        ACCOUNT_TYPE -- PA_ACCT
    ,to_number(lt.TRANSP_CLASS) DEVICE_CODED_CLASS  -- Convert to Number from varchar
    ,0 DEVICE_AGENCY_CLASS
    ,0 DEVICE_IAG_CLASS
    ,0 DEVICE_AXLES
    ,kl.ACCT_NUM ETC_ACCOUNT_ID  -- KS_LEDGER - Join KS_LEDGER on TXN_ID
    ,0 ACCOUNT_AGENCY_ID
    ,'N' READ_AVI_CLASS
    ,'N' READ_AVI_AXLES
    ,0 DEVICE_PROGRAM_STATUS
    ,0 BUFFERED_READ_FLAG
    ,NVL(MSG_INVALID,0) LANE_DEVICE_STATUS
    ,0 POST_DEVICE_STATUS
    ,(kl.BALANCE-kl.AMOUNT) PRE_TXN_BALANCE -- Derived - KS_LEDGER current balance minus amount 

------ PLAN_TYPE_ID - IF ITOL_AGENCY_CODE IN (9,10,11,12,13,14,15) 
------ THEN RENTAL ELSE POSTPAID, Xerox will provide numbers for  rental vs postpaid
--    ,CASE WHEN ITOL_AGENCY_CODE IN (9,10,11,12,13,14,15) 
--          THEN 'RENTAL' ELSE 'POSTPAID'
--      END PLAN_TYPE_ID
    ,NULL PLAN_TYPE_ID
    ,NULL ETC_TX_STATUS -- IF RENTAL THEN 9 of POSTPAID THEN 10 (From Above)

    ,'F' SPEED_VIOL_FLAG
    ,'Y' IMAGE_TAKEN
---- PLATE_COUNTRY - LOOKUP PA_STATE_CODE.  (visit DM_VEHICLE_INFO and DM_ACCT_INFO)
---- Xerox - internal lookup in DM_ADDRESS_INFO and assign country correctly. 
--    ,NULL PLATE_COUNTRY -- - LOOKUP PA_STATE_CODE. 

    ,(select STATE_CODE_ABBR from PA_STATE_CODE where STATE_CODE_NUM=lt.STATE_ID_CODE)
        PLATE_STATE  -- JOIN TO PA_STATE_CODE RETURN STATE_CODE_ABBR
    ,VEH_LIC_NUM PLATE_NUMBER
    ,trunc(EXT_DATE_TIME) REVENUE_DATE -- Date only
    ,trunc(TXN_PROCESS_DATE) POSTED_DATE -- Date only
    ,0 ATP_FILE_ID
    ,TXN_PROCESS_DATE UPDATE_TS -- Date and Time
    ,'N' IS_REVERSED
    ,0 CORR_REASON_ID
    ,trunc(TXN_PROCESS_DATE) RECON_DATE -- Date only
    ,NULL RECON_STATUS_IND  -- XEROX - TO FOLLOW UP
    ,NULL RECON_SUB_CODE_IND  -- XEROX - TO FOLLOW UP
    ,(select pt.COLLECTION_DATE from PA_TOUR pt where pt.TOUR_SEQ = lt.TOUR_TOUR_SEQ)
        EXTERN_FILE_DATE  -- JOIN TO PA_TOUR ON TOUR_SEQ=TOUR_TOUR_SEQ RETURN COLLECTION_DATE
    ,0 MILEAGE
    ,0 DEVICE_READ_COUNT
    ,0 DEVICE_WRITE_COUNT
    ,0 ENTRY_DEVICE_READ_COUNT
    ,0 ENTRY_DEVICE_WRITE_COUNT
    ,'000000000000' DEPOSIT_ID
    ,trunc(EXT_DATE_TIME) TX_DATE -- Date only
    ,pp.PLAZA_NAME LOCATION  -- JOIN PA_PLAZA ON PLAZA_ID=EXT_PLAZA_ID RETURN PLAZA_NAME
    ,pp.FACCODE_FACILITY_CODE FACILITY_ID -- JOIN PA_PLAZA ON PLAZA_ID=EXT_PLAZA_ID RETURN FACCODE_FACILITY_CODE
    ,0 FARE_TBL_ID
    ,0 TRAN_CODE
    ,'SUNTOLL' SOURCE_SYSTEM
    ,TXN_ID LANE_TX_ID
    ,NULL DEVICE_INTERNAL_NUMBER
FROM PA_LANE_TXN lt
    ,KS_LEDGER kl
    ,PA_PLAZA pp
WHERE lt.txn_id = kl.PA_LANE_TXN_ID
AND lt.EXT_PLAZA_ID = pp.PLAZA_ID (+)
AND lt.TRANSP_ID like '%2010'     --AND TRANSPONDER_ID ENDS WITH '2010' 
--AND  kl.ACCT_NUM >= p_begin_acct_num AND   kl.ACCT_NUM <= p_end_acct_num
; -- Source

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
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_TOLL_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_TOLL_INFO_tab.first .. DM_ACCOUNT_TOLL_INFO_tab.last
           INSERT INTO DM_ACCOUNT_TOLL_INFO VALUES DM_ACCOUNT_TOLL_INFO_tab(i);
                       
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



