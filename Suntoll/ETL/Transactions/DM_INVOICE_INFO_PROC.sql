/********************************************************
*
* Name: DM_INVOICE_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_INVOICE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_INVOICE_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_INVOICE_INFO_TYP IS TABLE OF DM_INVOICE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_INVOICE_INFO_tab DM_INVOICE_INFO_TYP;

P_ARRAY_SIZE NUMBER:=1000;


CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT --distinct
    ACCT_NUM ACCOUNT_NUMBER
    ,DOCUMENT_ID INVOICE_NUMBER
    ,trunc(CREATED_ON) INVOICE_DATE  -- date only   
    ,'CLOSED' STATUS  -- Derived (IN ETL)
    ,'0' ESCALATION_LEVEL  -- Derived (IN ETL)
      
    ,DOCUMENT_START START_DATE
    ,DOCUMENT_END END_DATE
    ,nvl(PREV_DUE,0) OPENING_BALANCE
    ,(nvl(PREV_DUE,0)
        + nvl(TOLL_CHARGED,0)
        + nvl(FEE_CHARGED,0) 
        + nvl(PAYMT_ADJS,0)) CLOSING_BALANCE  
    ,0 CREDITS  -- Derived (IN ETL)   
    ,(nvl(TOLL_CHARGED,0) + nvl(FEE_CHARGED,0)) PAYABALE
    ,(nvl(PREV_DUE,0) 
        + nvl(TOLL_CHARGED,0) 
        + nvl(FEE_CHARGED,0) 
        + nvl(PAYMT_ADJS,0)) INVOICE_AMT
    ,0 PAYMENTS -- Derived (IN ETL)  
    ,nvl(PAYMT_ADJS,0) ADJUSTMENTS
    ,'N' IS_ESCALATION_EXMPT
    ,nvl2(MAILED_ON, trunc(MAILED_ON)+25, to_date('01-01-1900','MM-DD-YYYY')) PAYMENT_DUE_DT  -- Default if NULL ?
    ,NULL DISCOUNT_ELIGIBLE_CHARGES
    ,0 DISCOUNTS
    ,0 MILEGE
    ,decode(NVL(DOC_TYPE_ID,0),0,'INVOICE',DOC_TYPE_ID) INVOICE_TYPE
    ,CREATED_ON CREATED
    ,'9986' CREATED_BY
    ,CREATED_ON LAST_UPD
    ,NULL LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
    ,COVER_IMAGE_VIO_EVENT_ID COVER_IMAGE_VIO_EVENT_ID 
    ,ADDRESS_ID ADDRESS_ID  -- PA_ACCT_ADDR.ADDRESS_ID
    ,VEH_LIC_NUM LICENSE_PLATE_NUM   -- PA_LANE_TXN.VEH_LIC_NUM
    ,MAILED_ON MAILED_ON 
    ,NULL DISMISSED   -- DISMISSED_AMT
    ,REG_STOP_FLAG REGISTRATION_STOP_FLAG   -- ST_ACCOUNT_FLAGS.REGISTRATION_STOP_FLAG
    ,RED_ENVELOPE_FLAG RED_ENVELOPE_FLAG   -- ST_ACCOUNT_FLAGS.RED_ENVELOPE_FLAG  
  FROM ST_DOCUMENT_INFO
--WHERE   ACCT_NUM >= p_begin_acct_num AND   ACCT_NUM <= p_end_acct_num
--where DOCUMENT_ID in (select DOCUMENT_ID from dup_invoice_num)
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
    FETCH C1 BULK COLLECT INTO DM_INVOICE_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN */
    FOR i IN 1 .. DM_INVOICE_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_INVOICE_INFO_tab(i).ACCOUNT_NUMBER;
      end if;

----VB_ACTIVITY table 
----FOR BANKRUPTCY_FLAG NULL and COLL_COURT_FLAG NULL
----IF DOCUMENT_ID is null, then 'UNBILLED'
----IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NULL, then 'INVOICED'
----IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL, then 'ESCALATED'
----IF DOCUMENT_ID is not null and CHILD_DOC_ID like '%-%', then 'UTC'
----FOR BANKRUPTCY_FLAG NULL and COLL_COURT_FLAG NOT NULL
----IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL  and COLL_COURT_FLAG is 'COLL' THEN 'COLLECTION'
----IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL  and COLL_COURT_FLAG is 'CRT' THEN 'COURT'
----FOR BANKRUPTCY _FLAG NOT NULL
----IF BANKRUPTCY _FLAG is not null, then 'BANKRUPTCY'     

      begin
--  DOCUMENT_ID is NOT null status  -----
        select distinct
        CASE WHEN BANKRUPTCY_FLAG is NOT null THEN 'BANKRUPTCY'
          WHEN COLL_COURT_FLAG is null      and CHILD_DOC_ID is null THEN 'INVOICED'
          WHEN COLL_COURT_FLAG is null      and CHILD_DOC_ID like '%-%' THEN 'UTC'
          WHEN COLL_COURT_FLAG is null      and CHILD_DOC_ID is NOT null THEN 'ESCALATED'
          WHEN COLL_COURT_FLAG is NOT null  and CHILD_DOC_ID is NOT null and COLL_COURT_FLAG = 'COLL' THEN 'COLLECTION'
          WHEN COLL_COURT_FLAG is NOT null  and CHILD_DOC_ID is NOT null and COLL_COURT_FLAG = 'CRT' THEN 'COURT'
--          WHEN BANKRUPTCY_FLAG is NULL THEN 'REG STOP' -- TODO: Need to add criteria for reg stop 
          ELSE '0'
         END    
        into  DM_INVOICE_INFO_tab(i).ESCALATION_LEVEL
        from  VB_ACTIVITY
        where DOCUMENT_ID = DM_INVOICE_INFO_tab(i).INVOICE_NUMBER
        and rownum=1  -- Verify what record to get ??
        ;
        
      exception 
        when no_data_found then --null;  OCUMENT_ID is null 
-- WHEN COLL_COURT_FLAG is null      and DOCUMENT_ID is null     THEN 'UNBILLED'
          DM_INVOICE_INFO_tab(i).ESCALATION_LEVEL := 'UNBILLED';
        when others then --null;
          DBMS_OUTPUT.PUT_LINE('1) INVOICE_NUMBER: '||DM_INVOICE_INFO_tab(i).INVOICE_NUMBER);
          insert into dup_invoice_num values (DM_INVOICE_INFO_tab(i).INVOICE_NUMBER);
          DM_INVOICE_INFO_tab(i).ESCALATION_LEVEL := '0';
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
           DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;

--  JOIN DOCUMENT_INFO_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of VB_ACTIVITY 
--  returns UNPAID_TXN_DETAILS
--UNION ALL 
--  JOIN DOCUMENT_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of ST_ACTIVITY_PAID 
--  returns PAID_TXN_DETAILS

-- IF UNPAID_AMT > 0 THEN 'OPEN' ELSE 'CLOSED'  --  (Need PAID_TXN_DETAILS definition from FTE); 

----    ,CASE WHEN (nvl(AMT_CHARGED,0) - nvl(TOTAL_AMT_PAID,0)) > 0 THEN 'OPEN'
----          WHEN (select nvl(AMOUNT,0) from KS_LEDGER where id = ap.LEDGER_ID)
----                - (nvl(ap.AMT_CHARGED,0) - nvl(ap.TOTAL_AMT_PAID,0)) >0  THEN 'DISPUTED'
----          ELSE 'CLOSED'
----      END STATUS  -- Derived

---- PAID_AMT + DISMISSED_AMT  -- What is  DISMISSED_AMT ??
--    ,(nvl(va.TOTAL_AMT_PAID,0) + nvl(ap.TOTAL_AMT_PAID,0))
--        + (nvl(ap.AMT_CHARGED,0) - nvl(ap.TOTAL_AMT_PAID,0)) 
--        + (nvl(va.AMT_CHARGED,0) - nvl(va.TOTAL_AMT_PAID,0)) CREDITS  -- Derived

------ PAID_AMT + DISMISSED_AMT  -- What is  DISMISSED_AMT ??
--    --    ,(nvl(va.TOTAL_AMT_PAID,0) + nvl(ap.TOTAL_AMT_PAID,0)) PAYMENTS 

        begin
--          select --distinct
--                  CASE WHEN sum(nvl(AMT_CHARGED,0) - nvl(TOTAL_AMT_PAID,0)) > 0 
--                      THEN 'OPEN'
----                    WHEN (select nvl(AMOUNT,0) from KS_LEDGER where id = ap.LEDGER_ID)
----                - (nvl(ap.AMT_CHARGED,0) - nvl(ap.TOTAL_AMT_PAID,0)) >0  THEN 'DISPUTED'
--                      ELSE 'CLOSED'
--                  END
--                ,sum(nvl(AMT_CHARGED,0) - nvl(TOTAL_AMT_PAID,0)) 
--                ,sum(nvl(TOTAL_AMT_PAID,0))
                
          select --distinct
                  CASE WHEN sum(nvl(AMT_CHARGED,0) - nvl(TOTAL_AMT_PAID,0)) > 0 
                      THEN 'OPEN'
--                    WHEN (select nvl(AMOUNT,0) from KS_LEDGER where id = ap.LEDGER_ID)
--                - (nvl(ap.AMT_CHARGED,0) - nvl(ap.TOTAL_AMT_PAID,0)) >0  THEN 'DISPUTED'
                      ELSE 'CLOSED'
                  END
                ,sum(nvl(AMT_CHARGED,0) - nvl(TOTAL_AMT_PAID,0)) 
                ,sum(nvl(TOTAL_AMT_PAID,0))

          into  DM_INVOICE_INFO_tab(i).STATUS
                ,DM_INVOICE_INFO_tab(i).CREDITS
                ,DM_INVOICE_INFO_tab(i).PAYMENTS
          from  
          ((select 
--                  COLL_COURT_FLAG
--                  ,DOCUMENT_ID
--                  ,CHILD_DOC_ID
--                  ,
                  AMT_CHARGED 
                  ,TOTAL_AMT_PAID
          from  VB_ACTIVITY
          where DOCUMENT_ID = DM_INVOICE_INFO_tab(i).INVOICE_NUMBER)
          union all
          (select 
--          COLL_COURT_FLAG
--                  ,DOCUMENT_ID
--                  ,CHILD_DOC_ID
                  AMT_CHARGED 
                  ,TOTAL_AMT_PAID
          from  ST_ACTIVITY_PAID
          where DOCUMENT_ID = DM_INVOICE_INFO_tab(i).INVOICE_NUMBER))
          ;
        exception 
        when no_data_found then --null;
          DM_INVOICE_INFO_tab(i).STATUS := 'CLOSED';
          DM_INVOICE_INFO_tab(i).CREDITS := 0;
          DM_INVOICE_INFO_tab(i).PAYMENTS := 0;
        when others then --null;
          DBMS_OUTPUT.PUT_LINE('1) INVOICE_NUMBER: '||DM_INVOICE_INFO_tab(i).INVOICE_NUMBER);
          insert into dup_invoice_pay_num values (DM_INVOICE_INFO_tab(i).INVOICE_NUMBER);
          DM_INVOICE_INFO_tab(i).STATUS := 'CLOSED';
          DM_INVOICE_INFO_tab(i).CREDITS := 0;
          DM_INVOICE_INFO_tab(i).PAYMENTS := 0;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
           DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
        end;

--  JOIN LEDGER_ID OF ST_ACTIVITY_PAID to ID OF KS_LEDGER 
--  returns ORIGINAL_AMT from KS_LEDGER AND DISPUTED_AMT FROM ST_ACTIVITY_PAID; 
--  Use idfference to determine if dismissed or not 
--  (IF 0 THEN 'DISPUTED' ELSE 'CLOSED') 
---- DISPUTED_AMT ??
----          WHEN (select nvl(AMOUNT,0) from KS_LEDGER where id = ap.LEDGER_ID)
----                - (nvl(ap.AMT_CHARGED,0) - nvl(ap.TOTAL_AMT_PAID,0)) >0  THEN 'DISPUTED'

      if DM_INVOICE_INFO_tab(i).STATUS != 'OPEN' then
        begin
          select CASE WHEN sum(nvl(kl.AMOUNT,0) - (nvl(sap.AMT_CHARGED,0) - nvl(sap.TOTAL_AMT_PAID,0))) > 0
                      THEN 'DISPUTED'         
                      ELSE 'CLOSED'
                  END 
          into  DM_INVOICE_INFO_tab(i).STATUS
          from  ST_ACTIVITY_PAID sap,
                KS_LEDGER kl
          where sap.DOCUMENT_ID = DM_INVOICE_INFO_tab(i).INVOICE_NUMBER
          and   sap.LEDGER_ID = kl.id
          ;
        exception 
          when others then null;
          DM_INVOICE_INFO_tab(i).STATUS := 'CLOSED';
        end;
      end if;
      IF DM_INVOICE_INFO_tab(i).CREDITS is null then
        DM_INVOICE_INFO_tab(i).CREDITS := 0;
      end if;
      IF DM_INVOICE_INFO_tab(i).PAYMENTS is null then
        DM_INVOICE_INFO_tab(i).PAYMENTS := 0;
      end if;
      
      v_trac_etl_rec.track_last_val := DM_INVOICE_INFO_tab(i).ACCOUNT_NUMBER;
      v_trac_etl_rec.end_val := DM_INVOICE_INFO_tab(i).ACCOUNT_NUMBER;

    END LOOP;

-- ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_INVOICE_INFO_tab.first .. DM_INVOICE_INFO_tab.last
           INSERT INTO DM_INVOICE_INFO VALUES DM_INVOICE_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
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

grant execute on DM_INVOICE_INFO_PROC to public;
commit;

