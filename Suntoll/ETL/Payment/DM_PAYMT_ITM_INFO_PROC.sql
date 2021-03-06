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
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    'UNDEFINED' CATEGORY        -- in ETL
    ,'UNDEFINED' SUB_CATEGORY   -- in ETL
    ,'UNDEFINED' TRANS_TYPE     -- in ETL
    ,0 AMOUNT       -- in ETL
    ,'FTE' ENDO_AGENCY
    ,'FTE' REVENUE_AGENCY 
    ,NULL DESCRIPTION  -- in ETL
    ,NULL TRANSFER_REASON
    ,NULL NOTICE_NUMBER
    ,NULL SEQUENCE_NUMBER
    ,PUR_ID PARENT_PAYMENT_ID   -- PA_PURCHASE
    ,PUR_TRANS_DATE CREATED   -- PA_PURCHASE  
    ,NULL CREATED_BY   -- in ETL
    ,SYSDATE LAST_UPD  -- N/A
    ,NULL LAST_UPD_BY -- N/A
    ,PUR_ID INVOICE_NUM   -- PA_PURCHASE
    ,NULL INVOICE_REF_ID
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_PURCHASE 
WHERE ACCT_ACCT_NUM >= p_begin_acct_num AND ACCT_ACCT_NUM <= p_end_acct_num
and   ACCT_ACCT_NUM >0
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

  OPEN C1(v_trac_rec.begin_acct,v_trac_rec.end_acct);  
--  v_trac_etl_rec.begin_val := v_trac_rec.begin_acct;
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PAYMT_ITM_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN */
    FOR i IN 1 .. DM_PAYMT_ITM_INFO_tab.COUNT LOOP
--      IF i=1 then
--        v_trac_etl_rec.BEGIN_VAL := DM_PAYMT_ITM_INFO_tab(i).ACCOUNT_NUMBER;
--      end if;

      begin
        select nvl(PRODUCT_PUR_PRODUCT_CODE,'UNDEFINED')
              ,nvl(PROD_AMT,0)
        into  DM_PAYMT_ITM_INFO_tab(i).CATEGORY
              ,DM_PAYMT_ITM_INFO_tab(i).AMOUNT
        from  PA_PURCHASE_DETAIL
        where PUR_PUR_ID = DM_PAYMT_ITM_INFO_tab(i).INVOICE_NUM
        ;
      exception 
        when others then null;
        DM_PAYMT_ITM_INFO_tab(i).CATEGORY := 'UNDEFINED';
        DM_PAYMT_ITM_INFO_tab(i).AMOUNT := 0;
      end;
        DM_PAYMT_ITM_INFO_tab(i).SUB_CATEGORY := DM_PAYMT_ITM_INFO_tab(i).CATEGORY;
        DM_PAYMT_ITM_INFO_tab(i).TRANS_TYPE := DM_PAYMT_ITM_INFO_tab(i).CATEGORY;
      
      begin
        select nvl(PROD_ABBRV,'UNDEFINED')
              ,PUR_PRODUCT_DESC
        into  DM_PAYMT_ITM_INFO_tab(i).TRANS_TYPE
              ,DM_PAYMT_ITM_INFO_tab(i).DESCRIPTION
        from  PA_PUR_PRODUCT
        where PUR_PRODUCT_CODE = DM_PAYMT_ITM_INFO_tab(i).CATEGORY
        ;
      exception 
        when others then null;
        DM_PAYMT_ITM_INFO_tab(i).TRANS_TYPE := 'UNDEFINED';
        DM_PAYMT_ITM_INFO_tab(i).DESCRIPTION := NULL;
      end;
            
      begin
        select EMP_EMP_CODE                 --,'UNDEFINED')
        into  DM_PAYMT_ITM_INFO_tab(i).CREATED_BY
        from  PA_PURCHASE_PAYMENT
        where PUR_PUR_ID = DM_PAYMT_ITM_INFO_tab(i).INVOICE_NUM
        ;
      exception 
        when others then null;
        DM_PAYMT_ITM_INFO_tab(i).CREATED_BY := NULL; -- 'UNDEFINED';
      end;
      
--      v_trac_etl_rec.track_last_val := DM_PAYMT_ITM_INFO_tab(i).ACCOUNT_NUMBER;
    END LOOP;
     /* ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_PAYMT_ITM_INFO_tab.first .. DM_PAYMT_ITM_INFO_tab.last
           INSERT INTO DM_PAYMT_ITM_INFO VALUES DM_PAYMT_ITM_INFO_tab(i);
           
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    v_trac_etl_rec.end_val := v_trac_etl_rec.track_last_val;
    update_track_proc(v_trac_etl_rec);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Total load count : '||ROW_CNT);

  COMMIT;

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
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

grant execute on DM_PAYMT_ITM_INFO_PROC to public;
commit;



