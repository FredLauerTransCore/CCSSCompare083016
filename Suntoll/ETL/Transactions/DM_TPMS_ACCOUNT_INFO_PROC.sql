/********************************************************
*
* Name: DM_TPMS_ACCOUNT_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_TPMS_ACCOUNT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_TPMS_ACCOUNT_INFO_PROC   
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_TPMS_ACCOUNT_INFO_TYP IS TABLE OF DM_TPMS_ACCOUNT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_TPMS_ACCOUNT_INFO_tab DM_TPMS_ACCOUNT_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    kal.ACCT_NUM ETC_ACCOUNT_ID  -- DERIVED?  from ACCOUNT_NO 
    ,'CCSS' AGENCY_ID
    ,NULL ACCOUNT_TYPE  -- PA_ACCT.ACCTTYPE_ACCT_TYPE_CODE
    
-- Anything with a positive balance is considered prepaid, 
    ,case when kal.CUST_BALANCE > 0 then kal.CUST_BALANCE
     else 0
     end CURRENT_BALANCE  

-- anything with a negative balance is considered postpaid.
    ,case when kal.CUST_BALANCE < 0 then kal.CUST_BALANCE
     else 0
     end POSTPAID_BALANCE  
    
    ,0 DEVICE_DEPOSIT
    ,0 VIOL_PREPAYMENT
    ,0 VIOL_OVERPAYMENT
    ,'N' IS_DISCOUNT_ELIGIBLE
    
-- DERIVED in ETL 
    ,NULL LAST_TOLL_TX_ID 
    ,NULL LAST_TOLL_POSTED_DATE
    ,NULL LAST_TOLL_TX_AMT
    ,NULL LAST_TOLL_TX_TIMESTAMP
    ,NULL FIRST_TOLL_TIMESTAMP

    ,to_date('01-01-1900','MM-DD-YYYY') EVAL_START_DATE
    ,to_date('01-01-1900','MM-DD-YYYY') EVAL_END_DATE
    ,0 USAGE_AMOUNT
    ,kal.LAST_UPDATED UPDATE_TS
    ,NULL OPEN_DATE     --  PA_ACCT.ACCT_OPEN_DATE
    ,0 POSTPAID_UNBILLED_BALANCE
    ,0 VIOL_TOLL_BALANCE
    ,0 VIOL_FEE_BALANCE
    ,0 VIOL_BALANCE
    
--    ,NULL POSTPAID_FEE
--    ,NULL COLL_TOLL
--    ,NULL COLL_FEE
--    ,NULL UTC_TOLL
--    ,NULL UTC_FEE
--    ,NULL COURT_TOLL
--    ,NULL COURT_FEE
    
-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns EXT_DATE_TIME (Date Portion)
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion)
    ,NULL LAST_TOLL_TX_DATE
    ,NULL LAST_FIN_TX_DATE
    
    ,0 ACCT_FIN_STATUS

-- KS_SUB_LEDGER_EVENTS and KS_LEDGER and GROUP_CODE = 1 
-- returns MAX(LEDGER_ID) WHERE EVENT_TYPE_ID IN 
-- (13, 56, 40, 26, 10, 57, 66, 115, 29, 14, 15, 67, 96)    
    ,NULL LAST_PAYMENT_TX_ID

-- POSTED_DATE OF KS_SUB_LEDGER and KS_LEDGER and GROUP_CODE = 1 
-- returns MAX(LEDGER_ID) WHERE EVENT_TYPE_ID IN  
--(13, 56, 40, 26, 10, 57, 66, 115, 29, 14, 15, 67, 96)    
    ,NULL LAST_PAYMENT_DATE

-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID)
    ,NULL LAST_FIN_TX_ID
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion)
    ,NULL LAST_FIN_TX_AMT
    ,to_date('01-01-1900','MM-DD-YYYY') LAST_BAL_POS_DATE
    ,to_date('01-01-1900','MM-DD-YYYY') LAST_BAL_NEG_DATE
    ,to_date('01-01-1900','MM-DD-YYYY') LAST_VIOL_PAYMENT_DATE
    ,0 POSTPAID_OVERPAYMENT

-- KS_LEDGER  BALANCE where MAX(POSTED_DATE) <= (END OF MONTH DAY YEAR)
    ,NULL LAST_STMT_CLOSE_BALANCE
-- KS_LEDGER POSTED_DATE where MAX(POSTED_DATE) <= (END OF MONTH DAY YEAR)
    ,NULL LAST_STMT_DATE
    
    ,NULL ACCOUNT_CCU       -- PA_ACCT_DETAIL.CA_AGENCY_ID
    ,'SUNTOLL' SOURCE_SYSTEM
    ,kal.LEDGER_ID LEDGER_ID
FROM KS_ACCT_LEDGER kal
WHERE   kal.ACCT_NUM >= p_begin_acct_num AND   kal.ACCT_NUM <= p_end_acct_num
; 

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

  OPEN C1(v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_TPMS_ACCOUNT_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /* ETL SECTION BEGIN*/
    FOR i IN 1 .. DM_TPMS_ACCOUNT_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID;
      end if;

-- DERIVED in ETL KS_ACCT_LEDGER AND KS_LEDGER 
-- returns MAX(PA_LANE_TXN_ID)
-- and POSTED_DATE and AMOUNT
-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns EXT_DATE_TIME (Time Portion)
-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns MIN(EXT_DATE_TIME) 
-- ** Based on available Activity, not actual first toll in the system for account (Time Portion)

--    ,NULL FIRST_TOLL_TIMESTAMP
      
      begin
        select PA_LANE_TXN_ID
              ,trunc(POSTED_DATE)
              ,AMOUNT
              ,POSTED_DATE
        into  DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_ID
              ,DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_POSTED_DATE
              ,DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_AMT
              ,DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_TIMESTAMP
        from  KS_LEDGER
        where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
           and PA_LANE_TXN_ID = (select max(PA_LANE_TXN_ID)
                                  from KS_LEDGER
                                  where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID)
        ;
      exception 
        when no_data_found then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_ID:=0;       
        when others then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_ID:=0;
          DBMS_OUTPUT.PUT_LINE('ACCOUNT_NUMBER: '||DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID);
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('REB ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;

-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID)
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion)
      begin
        select PA_PUR_DET_ID
              ,AMOUNT
        into  DM_TPMS_ACCOUNT_INFO_tab(i).LAST_FIN_TX_ID
              ,DM_TPMS_ACCOUNT_INFO_tab(i).LAST_FIN_TX_AMT
        from  KS_LEDGER
        where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
           and PA_PUR_DET_ID = (select max(PA_PUR_DET_ID)
                                  from KS_LEDGER
                                  where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID)
        ;
      exception 
        when no_data_found then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_ID:=0;       
        when others then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_ID:=0;
          DBMS_OUTPUT.PUT_LINE('ACCOUNT_NUMBER: '||DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID);
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('REB ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;

--    ,NULL ACCOUNT_TYPE  -- PA_ACCT.ACCTTYPE_ACCT_TYPE_CODE
--    ,NULL OPEN_DATE     -- PA_ACCT.ACCT_OPEN_DATE
      begin
        select ACCTTYPE_ACCT_TYPE_CODE
              ,CREATED_ON
        into  DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_TYPE
              ,DM_TPMS_ACCOUNT_INFO_tab(i).OPEN_DATE
        from PA_ACCT
        where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
        ;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_TYPE:=null;
      end;

      v_trac_etl_rec.track_last_val := DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID;
      v_trac_etl_rec.end_val := DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID;

    END LOOP;

    /* ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_TPMS_ACCOUNT_INFO_tab.first .. DM_TPMS_ACCOUNT_INFO_tab.last
           INSERT INTO DM_TPMS_ACCOUNT_INFO VALUES DM_TPMS_ACCOUNT_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
    COMMIT;
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Total Load count : '||ROW_CNT);

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  EXCEPTION
  WHEN OTHERS THEN
    v_trac_etl_rec.result_code := SQLCODE;
    v_trac_etl_rec.result_msg := SQLERRM;
    v_trac_etl_rec.proc_end_date := SYSDATE;
    update_track_proc(v_trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('EMP ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS

commit;

