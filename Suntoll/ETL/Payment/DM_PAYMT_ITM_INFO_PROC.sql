/********************************************************
*
* Name: DM_PAYMT_ITM_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PAYMT_ITM_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PAYMT_ITM_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_PAYMT_ITM_INFO_TYP IS TABLE OF DM_PAYMT_ITM_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PAYMT_ITM_INFO_tab DM_PAYMT_ITM_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    nvl(pd.PRODUCT_PUR_PRODUCT_CODE,'NULL') CATEGORY
    ,nvl(pd.PRODUCT_PUR_PRODUCT_CODE,'NULL') SUB_CATEGORY
    ,nvl(pd.PRODUCT_PUR_PRODUCT_CODE,'NULL') TRANS_TYPE
--    ,(select PROD_ABBRV from PA_PUR_PRODUCT where PUR_PRODUCT_CODE = pd.PRODUCT_PUR_PRODUCT_CODE) TRANS_TYPE    
    ,nvl(pd.PROD_AMT,0) AMOUNT
    ,'FTE' ENDO_AGENCY
    ,'FTE' REVENUE_AGENCY
--    ,(select pur.PUR_PRODUCT_DESC from PA_PUR_PRODUCT pur
--        where pur.PUR_PRODUCT_CODE = pd.PRODUCT_PUR_PRODUCT_CODE) DESCRIPTION   
    ,'(select pur.PUR_PRODUCT_DESC from PA_PUR_PRODUCT pur where pur.PUR_PRODUCT_CODE = pd.PRODUCT_PUR_PRODUCT_CODE)' DESCRIPTION   
    ,NULL TRANSFER_REASON
    ,NULL NOTICE_NUMBER
    ,NULL SEQUENCE_NUMBER
    ,pp.PUR_ID PARENT_PAYMENT_ID   -- PA_PURCHASE
    ,pp.PUR_TRANS_DATE CREATED   -- PA_PURCHASE
--    ,pay.EMP_EMP_CODE CREATED_BY  -- OR below
    ,(select pay.EMP_EMP_CODE from PA_PURCHASE_PAYMENT pay
        where pay.PUR_PUR_ID = pp.PUR_ID) CREATED_BY   
    ,SYSDATE LAST_UPD  -- N/A
    ,NULL LAST_UPD_BY -- N/A
    ,pp.PUR_ID INVOICE_NUM   -- PA_PURCHASE
    ,NULL INVOICE_REF_ID
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_PURCHASE pp
    ,PA_PURCHASE_DETAIL pd
--    ,PA_PURCHASE_PAYMENT pay
WHERE pp.PUR_ID = pd.PUR_PUR_ID (+)
--AND   pp.PUR_ID = pay.PUR_PUR_ID (+)
; -- source

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
    FETCH C1 BULK COLLECT INTO DM_PAYMT_ITM_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_PAYMT_ITM_INFO_tab.first .. DM_PAYMT_ITM_INFO_tab.last
           INSERT INTO DM_PAYMT_ITM_INFO VALUES DM_PAYMT_ITM_INFO_tab(i);
           
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


