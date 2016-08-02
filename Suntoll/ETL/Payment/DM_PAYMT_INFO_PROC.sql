/********************************************************
*
* Name: DM_PAYMT_INFO_PROC
* Created by: RH, 5/17/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PAYMT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_PAYMT_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_PAYMT_INFO_TYP IS TABLE OF DM_PAYMT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PAYMT_INFO_tab DM_PAYMT_INFO_TYP;

P_ARRAY_SIZE NUMBER:=1000;

-- 6/14/16 JL Updated REVERSED and NSF_FEE mapping to NULL based on 6/10 discussion with FTE and Xerox.

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    pp.ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,pp.PUR_TRANS_DATE TX_DT  -- PA_PURCHASE
    ,trim(nvl(pay.PAYTYPE_PAYMENT_TYPE_CODE,'UNDEFINED')) PAY_TYPE
    ,'R' TRAN_TYPE  -- DETAIL  (IN ETL)  PA_PURCHASE_DETAIL.PRODUCT_PUR_PRODUCT_CODE
    ,nvl(pay.PUR_PAY_AMT,0) AMOUNT
    ,NULL REVERSED  -- Default
    ,to_char(to_date(pay.PUR_CREDIT_EXP_DATE,'MM/YY'),'MM') EXP_MONTH
    ,to_char(to_date(pay.PUR_CREDIT_EXP_DATE,'MM/YY'),'YYYY') EXP_YEAR
    ,trim(pay.PUR_CREDIT_ACCT_NUM) CREDIT_CARD_NUMBER
    ,trim(pay.CHK_ACCT_NUM) BANK_ACCOUNT_NUMBER
    ,trim(pay.ABA_NUMBER) BANK_ROUTING_NUMBER
    ,trim(pay.CHECK_NUM) CHECK_NUMBER
    ,NULL NSF_FEE -- Default
    ,decode(nvl(pay.PUR_PUR_ID,0),0,'NULL',pay.PUR_PUR_ID) PAYMENT_REFERENCE_NUM   -- unique    
    ,trim(pay.EMP_EMP_CODE) EMPLOYEE_NUMBER    
    ,nvl(pay.PUR_PAY_ID,0) TRANSACTION_ID  
    ,NULL ORG_TRANSACTION_ID -- DETAIL  (IN ETL) PA_PURCHASE_DETAIL.VES_REF_NUM
    ,NULL XREF_TRANSACTION_ID -- DETAIL  (IN ETL)
    ,pay.PUR_CREDIT_AUTH_NUM CC_RESPONSE_CODE
    ,pay.CC_GATEWAY_REF_ID EXTERNAL_REFERENCE_NUM
    ,NULL MISS_APPLIED
    ,NULL DESCRIPTION  -- No mapping
-- If REF_APPROVED_DATE is populated, then the refund status will be populated. 
-- REF_DECLINE_REASON_ID will get the status of the refund.
    ,(select CASE WHEN rr.REF_APPROVED_DATE IS NOT NULL THEN 
            decode(rr.REFUND_STATUS_ID, 
              1,'UNAPPROVED', 
              2,'APPROVED', 
              3,'PARTIAL', 
              4,'REJECTED',NULL) 
            END 
      from PA_REFUND_REQUEST rr
      where rr.PUR_PUR_ID = pay.PUR_PUR_ID)  REFUND_STATUS 
    ,NULL REFUND_CHECK_NUM    
    ,'N/A' TOD_ID -- N/A Required
    ,pp.PUR_TRANS_DATE CREATED -- PA_PURCHASE
    ,pay.EMP_EMP_CODE CREATED_BY
    ,trunc(SYSDATE) LAST_UPD  -- N/A
    ,NULL LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
    ,pay.CC_GATEWAY_REQ_ID CC_GATEWAY_REQ_ID
    ,pay.CC_TXN_REF_NUM CC_TXN_REF_NUM
FROM PA_PURCHASE pp
    ,PA_PURCHASE_PAYMENT pay
WHERE pp.PUR_ID = pay.PUR_PUR_ID
AND   pp.ACCT_ACCT_NUM >= p_begin_acct_num AND   pp.ACCT_ACCT_NUM <= p_end_acct_num
; -- source

v_PUR_DET_ID     PA_PURCHASE_DETAIL.PUR_DET_ID%TYPE := 0;
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
    FETCH C1 BULK COLLECT INTO DM_PAYMT_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN */
    FOR i IN 1 .. DM_PAYMT_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_PAYMT_INFO_tab(i).ACCOUNT_NUMBER;
      end if;
      
      begin
        select PUR_DET_ID
              ,PRODUCT_PUR_PRODUCT_CODE   --,'UNDEFINED')
              ,VES_REF_NUM                --,'UNDEFINED')
        into   v_PUR_DET_ID
              ,DM_PAYMT_INFO_tab(i).TRAN_TYPE
              ,DM_PAYMT_INFO_tab(i).ORG_TRANSACTION_ID
        from  PA_PURCHASE_DETAIL
        where PUR_PUR_ID = DM_PAYMT_INFO_tab(i).PAYMENT_REFERENCE_NUM
        AND   ITEM_ORDER = 1  -- ?? verify
        ;
      exception 
        when others then null;
        v_PUR_DET_ID := 0;
        DM_PAYMT_INFO_tab(i).TRAN_TYPE := NULL; -- 'UNDEFINED';
        DM_PAYMT_INFO_tab(i).ORG_TRANSACTION_ID := NULL; -- 'UNDEFINED';
      end;
      
      if v_PUR_DET_ID != 0 then
        begin
          select  varr.CHARGE_LEDGER_ID
          into    DM_PAYMT_INFO_tab(i).XREF_TRANSACTION_ID
          from    VB_ACTIVITY_RECOVERY_REF varr,  
                  KS_LEDGER ks
          where   varr.PAID_CUST_LEDGER_ID=ks.ID
          and     ks.PA_PUR_DET_ID=v_PUR_DET_ID
          ;
        exception 
          when others then null;
          DM_PAYMT_INFO_tab(i).XREF_TRANSACTION_ID := NULL;
        end;
      end if;

      v_trac_etl_rec.track_last_val := DM_PAYMT_INFO_tab(i).ACCOUNT_NUMBER;
--      v_trac_etl_rec.end_val := DM_PAYMT_INFO_tab(i).ACCOUNT_NUMBER;

    END LOOP;

     /* ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_PAYMT_INFO_tab.first .. DM_PAYMT_INFO_tab.last
           INSERT INTO DM_PAYMT_INFO VALUES DM_PAYMT_INFO_tab(i);
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
    COMMIT;
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Total load count : '||ROW_CNT);

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
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS

commit;

