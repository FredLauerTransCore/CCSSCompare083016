/********************************************************
*
* Name: DM_INVOICE_ITM_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_INVOICE_ITM_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_INVOICE_ITM_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_INVOICE_ITM_INFO_TYP IS TABLE OF DM_INVOICE_ITM_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_INVOICE_ITM_INFO_tab DM_INVOICE_ITM_INFO_TYP;

P_ARRAY_SIZE NUMBER:=100;

-- REG_STOP_THRESHOLD_AMT_IN_CENTS -- too long changed length
REG_STOP_THRESHOLD_AMT_CENTS NUMBER := 100000000; -- TODO: Need to get actual threshold from FTE


CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT --distinct
    ACCT_NUM ACCOUNT_NUMBER -- ST_DOCUMENT_MAILING_EXCEPTION
    ,DOCUMENT_ID INVOICE_NUMBER
    ,'POSTPAID' CATEGORY  -- DEFAULT to 'Postpaid'
    ,DOC_TYPE_ID  SUB_CATEGORY  -- to use in ETL and get actual value

--    ,CASE WHEN (va.AMT_CHARGED-va.TOTAL_AMT_PAID)>0 THEN 'OPEN'
--          WHEN st.TOTAL_AMT_PAID = 0 THEN 'DISPUTED'
--          ELSE 'CLOSED' END STATUS
    ,'CLOSED' STATUS  -- in ETL

    ,trunc(NVL(CREATED_ON,SYSDATE)) INVOICE_DATE

-- ,nvl((select kl.AMOUNT from KS_LEDGER kl where kl.ID = va.LEDGER_ID),0) PAYABLE 
    ,(nvl(PREV_DUE,0) +
      nvl(TOLL_CHARGED,0) +
      nvl(FEE_CHARGED,0) +
      nvl(PAYMT_ADJS,0)) PAYABLE
--    ,0 PAYABLE

    ,0 INVOICE_ITEM_NUMBER
    ,0 LANE_TX_ID  -- in ETL

    ,'Open' LEVEL_INFO  -- in ETL
    ,nvl(PROMOTION_status,'UNDEFINED') REASON_CODE  --PROMOTION_CODE in ICD
    ,'INVOICE-ITEM' INVOICE_TYPE

--    ,nvl(st.TOTAL_AMT_PAID,0) PAID_AMOUNT
    ,0 PAID_AMOUNT  -- (IN ETL)

    ,NULL CREATED   -- IN ETL
    ,'SUNTOLL_CSC_ID' CREATED_BY
    ,NULL LAST_UPD  -- IN ETL
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
    ,NULL DISMISSED_AMT
FROM ST_DOCUMENT_INFO
WHERE   ACCT_NUM >= p_begin_acct_num AND ACCT_NUM <= p_end_acct_num
and     ACCT_NUM >0
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

  OPEN C1(v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_INVOICE_ITM_INFO_tab
    LIMIT P_ARRAY_SIZE;

    FOR i IN 1 .. DM_INVOICE_ITM_INFO_tab.COUNT LOOP

      begin
        IF i=1 then
          v_trac_etl_rec.BEGIN_VAL := DM_INVOICE_ITM_INFO_tab(i).ACCOUNT_NUMBER;
        end if;

--  DBMS_OUTPUT.PUT_LINE('1) DM_INVOICE_ITM_INFO_tab('||i||').SUB_CATEGORY: '||DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY);

        select --distinct
        CASE WHEN va.BANKRUPTCY_FLAG is NOT null 
          THEN 'BKTY'  -- Bankruptcy flag in VB_Activity to BKTY to get this status, 
    
--OTHER (balance adjustments) - 
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('8') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '8'
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'OTHER'

--INVTOLL
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID in ('38','42') 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'INVTOLL'

--ESCTOLL
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID in ('38','42') 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'ESCTOLL'

--CLTOLL (collection toll)
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG = 'COLL' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Collection assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'COLL' and vac.KS_LEDGER_EVENT_TYPE_ID in ('38','42') 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'CLTOLL' --(collection toll)

--UTCTOLL
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID ='3'
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID in ('38','42') 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY = '3' -- DOC_TYPE_ID
          THEN 'UTCTOLL'

--COURTTOLL
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG = 'CRT' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Court assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'CRT' and vac.KS_LEDGER_EVENT_TYPE_ID in ('38','42') 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'COURTTOLL'

--COURTPLEATOLL
--Join VB_ACTIVITY.KS_LEDGER_EVENT_TYPE_ID ='89' and COLL_COURT_FLAG is null and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') 
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '89'  
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'COURTPLEATOLL'

--REGHOLDTOLL   ----------------------------------------------------------------  Clairification ?
-- Join ST_ACCT_FLAGS.ACCT_NUM = ST_document_info.acct_num  
--  and ST_ACCT_FLAGS.FLAG_TYPE ='9' and END_DATE is null, 
          WHEN (SELECT 'Y' 
                FROM ST_ACCT_FLAGS saf 
                WHERE saf.ACCT_NUM = DM_INVOICE_ITM_INFO_tab(i).ACCOUNT_NUMBER 
                and saf.FLAG_TYPE ='9' and END_DATE is null)='Y'
          THEN 'REGHOLDTOLL'

--   All the following tolls are on REG HOLD -----  Unclear ------------------------
          
----JOIN ID             of KS_LEDGER to LEDGER_ID of VB_ACTVITY       for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
----JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID    of PA_LANE_LANE_TXN and
----JOIN PLAZA_ID       of PA_PLAZA  to EXT_PLAZA_ID for PA_Lane_txn
--
-- when --IF SUM (VB_ACTIVITY.AMT_CHARGED – VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS grouped by KS_LEDGER.ACCT_NUM
----  FOR PA_PLAZA.FTE_PLAZA = 'Y',
----  VB_ACTIVITY.COLL_COURT_FLAG is NULL, 
----  VB_ACTIVITY.CHILD_DOC_ID not null,
----  VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and 
----  VB_ACTIVITY.BANKRUPTCY_flag is null
----          THEN 'REGSTOP'
--
--          WHEN va.COLL_COURT_FLAG is null and va.CHILD_DOC_ID not null 
----      va.CHILD_DOC_ID NOT LIKE '%-%' and 
--          
--          va.KS_LEDGER_EVENT_TYPE_ID = '89'  and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') 
--          THEN 'REGSTOP'
-------------------------------------------------------------------------------------


--ADMINFEE
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('48') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '48' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'ADMINFEE'

--ESCADMINFEE
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('48') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID = '48' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'ESCADMINFEE'

--CLADMINFEE
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('48') and COLL_COURT_FLAG = 'COLL' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Collection assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG  = 'COLL' and vac.KS_LEDGER_EVENT_TYPE_ID = '48' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'CLADMINFEE'

--UTCFEE
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('25') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID ='3'
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '25' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY = '3' -- DOC_TYPE_ID
          THEN 'UTCFEE'

--CRTPLEAFEE
--Join VB_ACTIVITY.KS_LEDGER_EVENT_TYPE_ID ='100' and COLL_COURT_FLAG is null and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '100' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'CRTPLEAFEE'

--COURTUTCFEE
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('25') and COLL_COURT_FLAG = 'CRT' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Court assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'CRT' and vac.KS_LEDGER_EVENT_TYPE_ID = '25' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'COURTUTCFEE'

--EXPLANECHARGE (express lane charge)
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('145') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '145' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'EXPLANECHARGE' --(express lane charge)

--ESCEXPPLANCECHARGE
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('145') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID = '145' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'ESCEXPPLANCECHARGE'

--MAILFEE (include a statement fee in case this is in the data)- We do not do this currently

--NSF
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('17') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '17' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'NSF'

--ESCNSF
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('17') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID = '17' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'ESCNSF'

--CLNSF
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('17') and COLL_COURT_FLAG = 'COLL' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Collection assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'COLL' and vac.KS_LEDGER_EVENT_TYPE_ID = '17' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'CLNSF'

--CLEXPLANECHARGE
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('145') and COLL_COURT_FLAG = 'COLL' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Collection assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'COLL' and vac.KS_LEDGER_EVENT_TYPE_ID = '145' 
            and DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY in ('1','2') -- DOC_TYPE_ID
          THEN 'CLEXPLANECHARGE'

--**Discussed bankruptcy, deceased, court – these will be tracked as statuses 
--For ST_document_info. Acct_num joined PA_ACCT_DETAIL.DECEASED_DATE -- CRT is provided above   
          ELSE 'INVPAYMENT'   -- ?
         END    
        into  DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY
        from  VB_ACTIVITY va
              ,VB_ACTIVITY vac  -- Child
        where va.DOCUMENT_ID = DM_INVOICE_ITM_INFO_tab(i).INVOICE_NUMBER
        and   va.CHILD_DOC_ID = vac.DOCUMENT_ID (+)
        and   rownum<=1  -- Verify what record to get ??
        ;
--  DBMS_OUTPUT.PUT_LINE('2) DM_INVOICE_ITM_INFO_tab('||i||').SUB_CATEGORY: '||DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY);
        exception 
          when no_data_found then
          DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY := 'INVPAYMENT';
          when others then null;
          DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY := 'Multi-value';
          DBMS_OUTPUT.PUT_LINE('2) INVOICE_NUMBER: '||DM_INVOICE_ITM_INFO_TAB(i).INVOICE_NUMBER);
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
--           DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
        end;
--  DBMS_OUTPUT.PUT_LINE('3) DM_INVOICE_ITM_INFO_tab('||i||').SUB_CATEGORY: '||DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY);

-- ST_DOCUMENT_INFO.PROMOTIONAL_STATUS 
-- SUM(PREV_DUE, TOLL_CHARGED, FEE_CHARGED, PAYMT_ADJS) 
 
-- Join ST_Document_info.document_id with VB_Activity.document_id  
-- and for same record 
-- join KS_LEDGER.ID to VB_activity.LEDGER_ID to get the KS_LEDGER.Amount( Original amount)

--    ,nvl((select kl.AMOUNT 
--          from KS_LEDGER kl
--           where kl.ID = va.LEDGER_ID),0) PAYABLE -- di.PREV_DUE + di.TOLL_CHARGED + di.FEE_CHARGED + di.PAYMT_ADJS) PAYABLE
--    ,nvl((select unique kl.PA_LANE_TXN_ID from KS_LEDGER kl
--        where kl.ID = va.LEDGER_ID),0) LANE_TX_ID

        begin
          select --sum(nvl(kl.AMOUNT,0)),
                distinct kl.PA_LANE_TXN_ID
          into  --DM_INVOICE_ITM_INFO_tab(i).PAYABLE,
                DM_INVOICE_ITM_INFO_tab(i).LANE_TX_ID
          from  ST_ACTIVITY_PAID sap,
                KS_LEDGER kl
          where sap.DOCUMENT_ID = DM_INVOICE_ITM_INFO_tab(i).INVOICE_NUMBER
          and   sap.LEDGER_ID = kl.id
--          group by kl.PA_LANE_TXN_ID
          ;
        exception 
          when others then null;
--          DM_INVOICE_ITM_INFO_tab(i).PAYABLE := 0;
          DM_INVOICE_ITM_INFO_tab(i).LANE_TX_ID := 0;
        end;
        

-- LEVEL_INFO SET TO ----

/* VB_ACTIVITY table 
FOR BANKRUPTCY_FLAG NULL and COLL_COURT_FLAG NULL
IF DOCUMENT_ID is null, then 'UNBILLED'
IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NULL, then 'INVOICED'
IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL, then 'ESCALATED'
IF DOCUMENT_ID is not null and CHILD_DOC_ID like '%-%', then 'UTC'

FOR BANKRUPTCY_FLAG NULL and COLL_COURT_FLAG NOT NULL
IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL  and COLL_COURT_FLAG is 'COLL' THEN 'COLLECTION'
IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL  and COLL_COURT_FLAG is 'CRT' THEN 'COURT'

FOR BANKRUPTCY _FLAG NOT NULL
IF BANKRUPTCY _FLAG is not null, then 'BANKRUPTCY'
*/ 

      begin
        select -- distinct
        CASE WHEN BANKRUPTCY_FLAG is NOT null THEN 'DISMISSED' --'BANKRUPTCY'
          WHEN COLL_COURT_FLAG is null  and CHILD_DOC_ID is null     THEN 'INVOICED'
          WHEN COLL_COURT_FLAG is null  and CHILD_DOC_ID like '%-%'  THEN 'UTC'
          WHEN COLL_COURT_FLAG is null  and CHILD_DOC_ID is NOT null THEN 'ESCALATED'
          WHEN COLL_COURT_FLAG = 'COLL' and CHILD_DOC_ID is NOT null THEN 'COLLECTION'
          WHEN COLL_COURT_FLAG = 'CRT'  and CHILD_DOC_ID is NOT null THEN 'COURT'
--          WHEN BANKRUPTCY_FLAG is NULL THEN 'REG STOP' -- TODO: Need to add criteria for reg stop 
--          ELSE 'REG STOP'
          ELSE 'INVPAYMENT'
         END    
        into  DM_INVOICE_ITM_INFO_tab(i).LEVEL_INFO
        from  VB_ACTIVITY
        where DOCUMENT_ID = DM_INVOICE_ITM_INFO_tab(i).INVOICE_NUMBER
        and rownum<=1  -- Verify what record to get ??
        ;
      exception 
        when no_data_found then
-- WHEN COLL_COURT_FLAG is null      and DOCUMENT_ID is null     THEN 'UNBILLED'
          DM_INVOICE_ITM_INFO_tab(i).LEVEL_INFO := 'INVPAYMENT';
--          DM_INVOICE_ITM_INFO_tab(i).LEVEL_INFO := 'UNBILLED';
        when others then --null;
          DM_INVOICE_ITM_INFO_tab(i).LEVEL_INFO := 'INVPAYMENT';
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
           DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;


--JOIN DOCUMENT_INFO_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of VB_ACTIVITY 
--  returns UNPAID_TXN_DETAILS 
--UNION ALL 
--JOIN DOCUMENT_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of ST_ACTIVITY_PAID 
--  returns PAID_TXN_DETAILS 
--IF UNPAID_AMT > 0 THEN 'OPEN' ELSE 'CLOSED'  (Need PAID_TXN_DETAILS definition from FTE); 
        
        begin
          select  CASE WHEN sum(nvl(AMT_CHARGED,0) - nvl(TOTAL_AMT_PAID,0)) > 0 
                      THEN 'OPEN'
--                    WHEN (select nvl(AMOUNT,0) from KS_LEDGER where id = ap.LEDGER_ID)
--                - (nvl(ap.AMT_CHARGED,0) - nvl(ap.TOTAL_AMT_PAID,0)) >0  THEN 'DISPUTED'
                      ELSE 'CLOSED'
                  END
--                ,sum(nvl(AMT_CHARGED,0) - nvl(TOTAL_AMT_PAID,0)) 
                ,sum(nvl(TOTAL_AMT_PAID,0))
          into  DM_INVOICE_ITM_INFO_TAB(i).STATUS
 --               ,DM_INVOICE_ITM_INFO_TAB(i).CREDITS
                ,DM_INVOICE_ITM_INFO_TAB(i).PAID_AMOUNT
          from  
          ((select AMT_CHARGED 
                  ,TOTAL_AMT_PAID
          from  VB_ACTIVITY
          where DOCUMENT_ID = DM_INVOICE_ITM_INFO_TAB(i).INVOICE_NUMBER)
          union all
          (select AMT_CHARGED 
                  ,TOTAL_AMT_PAID
          from  ST_ACTIVITY_PAID
          where DOCUMENT_ID = DM_INVOICE_ITM_INFO_TAB(i).INVOICE_NUMBER))
          ;
        exception 
          when others then null;
          DBMS_OUTPUT.PUT_LINE('2) INVOICE_NUMBER: '||DM_INVOICE_ITM_INFO_TAB(i).INVOICE_NUMBER);
          DM_INVOICE_ITM_INFO_TAB(i).STATUS := 'CLOSED';
          DM_INVOICE_ITM_INFO_TAB(i).PAID_AMOUNT := 0;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
           DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
        end;        

--JOIN LEDGER_ID OF ST_ACTIVITY_PAID to ID OF KS_LEDGER 
--  returns ORIGINAL_AMT from KS_LEDGER AND DISPUTED_AMT FROM ST_ACTIVITY_PAID; 
--  Use idfference to determine if dismissed or not (IF 0 THEN 'DISPUTED' ELSE 'CLOSED') 

      if DM_INVOICE_ITM_INFO_tab(i).STATUS != 'OPEN' then
        begin
          select CASE WHEN sum(nvl(kl.AMOUNT,0) - (nvl(sap.AMT_CHARGED,0) - nvl(sap.TOTAL_AMT_PAID,0))) > 0
                      THEN 'DISPUTED'         
                      ELSE 'CLOSED'
                  END 
          into  DM_INVOICE_ITM_INFO_tab(i).STATUS
          from  ST_ACTIVITY_PAID sap,
                KS_LEDGER kl
          where sap.DOCUMENT_ID = DM_INVOICE_ITM_INFO_tab(i).INVOICE_NUMBER
          and   sap.LEDGER_ID = kl.id
          ;
        exception 
          when others then null;
          DM_INVOICE_ITM_INFO_tab(i).STATUS := 'CLOSED';
        end;
      end if;
      IF DM_INVOICE_ITM_INFO_tab(i).PAID_AMOUNT is null then
        DM_INVOICE_ITM_INFO_tab(i).PAID_AMOUNT := 0;
      end if;

      
      begin
        select CREATED_ON
              ,CREATED_ON
        into   DM_INVOICE_ITM_INFO_tab(i).CREATED
              ,DM_INVOICE_ITM_INFO_tab(i).LAST_UPD
        from  PA_ACCT
        where ACCT_NUM = DM_INVOICE_ITM_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when others then null;
        DM_INVOICE_ITM_INFO_tab(i).CREATED := SYSDATE;
        DM_INVOICE_ITM_INFO_tab(i).LAST_UPD := SYSDATE;
      end;

      if DM_INVOICE_ITM_INFO_tab(i).LANE_TX_ID is null then
        DM_INVOICE_ITM_INFO_tab(i).LANE_TX_ID := 0;
      End if;

      if DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY is null then
        DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY := 'INVPAYMENT';
      End if;

--    DBMS_OUTPUT.PUT_LINE('ACCT- '||i||' - '||DM_INVOICE_ITM_INFO_tab(i).ACCOUNT_NUMBER);

      v_trac_etl_rec.track_last_val := DM_INVOICE_ITM_INFO_tab(i).ACCOUNT_NUMBER;

    END LOOP;
 
    /*  ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_INVOICE_ITM_INFO_tab.first .. DM_INVOICE_ITM_INFO_tab.last
           INSERT INTO DM_INVOICE_ITM_INFO VALUES DM_INVOICE_ITM_INFO_tab(i);
                       
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

grant execute on DM_INVOICE_ITM_INFO_PROC to public;
commit;


