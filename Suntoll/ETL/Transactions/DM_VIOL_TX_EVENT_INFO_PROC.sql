/********************************************************
*
* Name: DM_VIOL_TX_EVENT_INFO_PROC
* Created by: RH, 5/23/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_VIOL_TX_EVENT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_VIOL_TX_EVENT_INFO_PROC 
(i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_VIOL_TX_EVENT_INFO_TYP IS TABLE OF DM_VIOL_TX_EVENT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_VIOL_TX_EVENT_INFO_tab DM_VIOL_TX_EVENT_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
 /** Internal Xerox lookup;  
 Valid status: (xerox internal values 0100 to 0200) 
 FTE:  Unbilled; Invoice One; Invoice Two; Collection; Registration Stop; Court; 
       Write Off; Bankruptcy; Deceased; UTC (Uniform Traffic Citation); Paid;  .  
 Sunny will provide extract rule to determine status once the status is finalized between Xerox and FTE. 
 **/
    0 EVENT_TYPE
    ,0 PREV_EVENT_TYPE
    ,NULL VIOL_TX_STATUS  -- in ETL
    ,0 PREV_VIOL_TX_STATUS
    ,EXT_DATE_TIME EVENT_TIMESTAMP -- Required
    ,0 ETC_ACCOUNT_ID   -- Join KS_LEDGER on TXN_ID  in ETL
    ,VEH_LIC_NUM PLATE_NUMBER
    ,STATE_ID_CODE  PLATE_STATE  -- JOIN TO PA_STATE_CODE RETURN STATE_CODE_ABBR
    ,'USA' PLATE_COUNTRY  -- default in ETL
    ,OAVA_LINK_ID MAKE_ID -- Use to get actual value in ETL  
    ,0 DMV_PLATE_TYPE     -- Derived  ?? VERIFY
    ,0 REVIEWED_VEHICLE_TYPE
    ,0 REVIEWED_CLASS
    ,3 VIOL_TYPE
    ,0 OUTPUT_FILE_ID
    ,'**' OUTPUT_FILE_TYPE
    ,EXT_DATE_TIME TX_TIMESTAMP   -- time only
    ,trunc(EXT_DATE_TIME) TX_DATE -- date only
    ,0 CITATION_LEVEL     -- Derived  
    ,0 CITATION_STATUS    -- Derived  
    ,0 NOTICE_FEE_AMOUNT  -- Derived    

 -- TOTAL_AMT_PAID JOIN WITH KS_LEDGER ON LEDGER_ID AND KS_LEDGER TO PA_LANE_TXN on PA_LANE_TXN_ID
 --    ,kl.AMOUNT AMOUNT_PAID ?
    ,TOLL_AMT_COLLECTED AMOUNT_PAID
    ,TOLL_AMT_CHARGED DISCOUNTED_AMOUNT
    ,0 IMAGE_BATCH_ID
    ,0 IMAGE_BATCH_SEQ_NUMBER
    ,0 RECON_STATUS_IND  -- XEROX - TO FOLLOW UP  Required, default 0?
    ,'SUNTOLL' SOURCE_SYSTEM
    ,txn_id LANE_TX_ID
FROM PA_LANE_TXN  lt
where txn_id = (select PA_LANE_TXN_ID
                from   KS_LEDGER kl2
                WHERE kl2.PA_LANE_TXN_ID = lt.txn_id
                AND   kl2.ACCT_NUM >= p_begin_acct_num AND kl2.ACCT_NUM <= p_end_acct_num
                and   rownum<=1
                )
;

v_ks_ledger_id   KS_LEDGER.ID%TYPE:= 0;
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
    FETCH C1 BULK COLLECT INTO DM_VIOL_TX_EVENT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */
    
    FOR i in 1 .. DM_VIOL_TX_EVENT_INFO_tab.count loop

      begin
        select  ID
               ,ACCT_NUM
        into    v_ks_ledger_id
               ,DM_VIOL_TX_EVENT_INFO_tab(i).ETC_ACCOUNT_ID
        from    KS_LEDGER -- k1
        where   PA_LANE_TXN_ID=DM_VIOL_TX_EVENT_INFO_tab(i).LANE_TX_ID
        and     rownum<=1
        and     POSTED_DATE = (select max(POSTED_DATE) 
                              from    KS_LEDGER -- k2
                              where   PA_LANE_TXN_ID=DM_VIOL_TX_EVENT_INFO_tab(i).LANE_TX_ID)
        ;
      exception 
        when others then null;
        DM_VIOL_TX_EVENT_INFO_tab(i).ETC_ACCOUNT_ID := 0;
      end;
      
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_VIOL_TX_EVENT_INFO_tab(i).ETC_ACCOUNT_ID;
      end if;

      begin
        select COUNTRY into DM_VIOL_TX_EVENT_INFO_tab(i).PLATE_COUNTRY 
        from  COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_VIOL_TX_EVENT_INFO_tab(i).PLATE_STATE;
      exception
        when others then null;
        DM_VIOL_TX_EVENT_INFO_tab(i).PLATE_COUNTRY:='USA';
      end;
 
      begin
        select 
/*
VB_ACTIVITY table (Now uses UNION of VB_ACTIVITY and 
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
"
**Translation for action values, convert to translated values below
*/
--   VIOL_TX_STATUS mapping:
--   =======================
--   DOCUMENT_ID   |  Value
--   -----------------------
--   UNBILLED      |  601
--   INVOICED      |  602
--   ESCALATED     |  603
--   UTC           |  604
--   COLLECTION    |  605
--   COURT         |  606
--   BANKRUPTCY    |  607
--   -----------------------

--          CASE WHEN BANKRUPTCY_FLAG is NOT NULL then 607 -- 'BANKRUPTCY'
--              WHEN DOCUMENT_ID IS NULL      THEN 601 -- 'UNBILLED'
--              WHEN CHILD_DOC_ID IS NULL     THEN 602 -- 'INVOICED'
--              WHEN COLL_COURT_FLAG = 'COLL' then 605 -- 'COLLECTION'
--              WHEN COLL_COURT_FLAG = 'CRT'  then 606 -- 'COURT'
--              WHEN CHILD_DOC_ID LIKE '%-%'  THEN 604 -- 'UTC'
--              WHEN CHILD_DOC_ID IS NOT NULL THEN 603 -- 'ESCALATED'
--              ELSE 0    -- NULL  Required Default 0?
--          END  

/*
-- From ICD
-- Also includes from previous comment --

JOIN ID of KS_LEDGER to LEDGER_ID of VB_ACTVITY for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID of PA_LANE_LANE_TXN and 
JOIN PLAZA_ID of PA_PLAZA to EXT_PLAZA_ID for PA_Lane_txn

  IF SUM (VB_ACTIVITY.AMT_CHARGED – VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS 
    grouped by KS_LEDGER.ACCT_NUM
  FOR PA_PLAZA.FTE_PLAZA = 'Y',
      VB_ACTIVITY.COLL_COURT_FLAG is NULL, 
      VB_ACTIVITY.CHILD_DOC_ID not null,
      VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and 
      VB_ACTIVITY.BANKRUPTCY_flag is null
  THEN 'REG STOP'     
*/
--        ,
        CASE WHEN BANKRUPTCY_FLAG is NOT null THEN 607 -- 'BANKRUPTCY'
              WHEN DOCUMENT_ID is null     and COLL_COURT_FLAG is null  THEN 601 -- 'UNBILLED'
              WHEN DOCUMENT_ID is NOT null and COLL_COURT_FLAG is null  and CHILD_DOC_ID is null     THEN 602 -- 'INVOICED'
              WHEN DOCUMENT_ID is NOT null and COLL_COURT_FLAG is null  and CHILD_DOC_ID is NOT null THEN 603 -- 'ESCALATED'
              WHEN DOCUMENT_ID is NOT null and COLL_COURT_FLAG is null  and CHILD_DOC_ID like '%-%'  THEN 604 -- 'UTC'
              WHEN DOCUMENT_ID is NOT null and CHILD_DOC_ID is NOT null and COLL_COURT_FLAG = 'COLL' THEN 605 -- 'COLLECTION'
              WHEN DOCUMENT_ID is NOT null and CHILD_DOC_ID is NOT null and COLL_COURT_FLAG = 'CRT'  THEN 606 -- 'COURT'
              ELSE 0  --   -- NULL  Required Default 0?
          END
      
---- JOIN DOCUMENT_ID OF ST_DOCUMENT_INFO WITH CHILD_DOC_ID
---- JOIN ST_DOCUMENT_INFO.DOCUMENT_ID = CHILD_DOC_ID
--IF STATUS = 'UTC' SET STATUS TO UTC_MAIL  GO TO MAILON ACTIVITY FOR MAILED_ON_DATE ELSE 0
        ,CASE WHEN COLL_COURT_FLAG is null and DOCUMENT_ID is NOT null and CHILD_DOC_ID like '%-%' 
              THEN 1  -- UTC_MAIL
              ELSE 0
          END
--IF STATUS = 'UTC' SET STATUS TO UTC_MAIL  GO TO MAILON ACTIVITY FOR MAILED_ON_DATE
        ,CASE WHEN COLL_COURT_FLAG is null and DOCUMENT_ID is NOT null and CHILD_DOC_ID like '%-%' 
              THEN 1  -- UTC_MAIL
              ELSE 0
          END
  -- IF VIOL_TX_STATUS = 'UTC' then $25 ELSE 0 
        ,CASE WHEN COLL_COURT_FLAG is null and DOCUMENT_ID is NOT null and CHILD_DOC_ID like '%-%' 
              THEN 25
              ELSE 0
          END 
        into  --DM_VIOL_TX_EVENT_INFO_tab(i).VIOL_TX_STATUS              --,
              DM_VIOL_TX_EVENT_INFO_tab(i).DMV_PLATE_TYPE             
              ,DM_VIOL_TX_EVENT_INFO_tab(i).CITATION_LEVEL
              ,DM_VIOL_TX_EVENT_INFO_tab(i).CITATION_STATUS
              ,DM_VIOL_TX_EVENT_INFO_tab(i).NOTICE_FEE_AMOUNT
        from  VB_ACTIVITY
        where LEDGER_ID = v_ks_ledger_id
        ;
      exception 
        when others then null;
--          DM_VIOL_TX_EVENT_INFO_tab(i).VIOL_TX_STATUS := 0;
          DM_VIOL_TX_EVENT_INFO_tab(i).DMV_PLATE_TYPE := 0;
          DM_VIOL_TX_EVENT_INFO_tab(i).CITATION_LEVEL := 0;
          DM_VIOL_TX_EVENT_INFO_tab(i).CITATION_STATUS := 0;
          DM_VIOL_TX_EVENT_INFO_tab(i).NOTICE_FEE_AMOUNT := 0;
      end;

/*
UNION of VB_ACTIVITY and 
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
"
**Translation for action values, convert to translated values below
*/
--   VIOL_TX_STATUS mapping:
--   =======================
--   DOCUMENT_ID   |  Value
--   -----------------------
--   UNBILLED      |  601
--   INVOICED      |  602
--   ESCALATED     |  603
--   UTC           |  604
--   COLLECTION    |  605
--   COURT         |  606
--   BANKRUPTCY    |  607
--   -----------------------
      begin
        select 
          CASE WHEN BANKRUPTCY_FLAG is NOT NULL then 607 -- 'BANKRUPTCY'
              WHEN DOCUMENT_ID IS NULL      THEN 601 -- 'UNBILLED'
              WHEN CHILD_DOC_ID IS NULL     THEN 602 -- 'INVOICED'
              WHEN COLL_COURT_FLAG = 'COLL' then 605 -- 'COLLECTION'
              WHEN COLL_COURT_FLAG = 'CRT'  then 606 -- 'COURT'
              WHEN CHILD_DOC_ID LIKE '%-%'  THEN 604 -- 'UTC'
              WHEN CHILD_DOC_ID IS NOT NULL THEN 603 -- 'ESCALATED'
              ELSE 0    -- NULL  Required Default 0?
          END  
        into  DM_VIOL_TX_EVENT_INFO_tab(i).VIOL_TX_STATUS              
--        from  ST_ACTIVITY_V 
--        where LEDGER_ID = v_ks_ledger_id
        from      
        (SELECT
          COLL_COURT_FLAG,
          BANKRUPTCY_FLAG,
          'Unpaid'         STATUS,
          document_id,
          child_doc_id
        FROM vb_activity
        WHERE ledger_id = v_ks_ledger_id
        UNION
        SELECT 
          COLL_COURT_FLAG,
          BANKRUPTCY_FLAG,
          'Paid'          STATUS,
          document_id,
          child_doc_id
        FROM st_activity_paid
        WHERE ledger_id = v_ks_ledger_id)
        ;
      exception 
        when others then null;
          DM_VIOL_TX_EVENT_INFO_tab(i).VIOL_TX_STATUS := 0;
      end;


      begin
        select nvl(ev.MAKE, 'UNDEFINED')
        into  DM_VIOL_TX_EVENT_INFO_tab(i).MAKE_ID
        from  EVENT_VEHICLE ev, 
              EVENT_OWNER_ADDR_VEHICLE_ACCT eo
        where ev.ID = eo.VEHICLE_ID
        and   eo.ID = DM_VIOL_TX_EVENT_INFO_tab(i).MAKE_ID -- OAVA_LINK_ID   
        -- and   rownum<=1
        ;
      exception 
        when no_data_found then null;
          DM_VIOL_TX_EVENT_INFO_tab(i).MAKE_ID := 'UNDEFINED';      
        when others then null;
          DM_VIOL_TX_EVENT_INFO_tab(i).MAKE_ID := 'UNDEFINED';      
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
           DBMS_OUTPUT.PUT_LINE('MAKE ERROR CODE: '||v_trac_etl_rec.result_code);
           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end; 
      
      /* get PA_LANE_TXN_REJECT.TXN_ID for LANE_TX_ID */  -- No Records in table
      begin
        select TXN_ID into DM_VIOL_TX_EVENT_INFO_tab(i).LANE_TX_ID
        from  PA_LANE_TXN_REJECT
        where TXN_ID=DM_VIOL_TX_EVENT_INFO_tab(i).LANE_TX_ID
              and rownum<=1;
        exception 
          when others then null;
          DM_VIOL_TX_EVENT_INFO_tab(i).LANE_TX_ID:=null;
      end;
      
      if DM_VIOL_TX_EVENT_INFO_tab(i).EVENT_TIMESTAMP is null then
         DM_VIOL_TX_EVENT_INFO_tab(i).EVENT_TIMESTAMP:=to_date('12319999','MMDDYYYY');
      end if;
      if DM_VIOL_TX_EVENT_INFO_tab(i).VIOL_TX_STATUS is null then
         DM_VIOL_TX_EVENT_INFO_tab(i).VIOL_TX_STATUS:='0';
      end if;
      if DM_VIOL_TX_EVENT_INFO_tab(i).MAKE_ID is null then
         DM_VIOL_TX_EVENT_INFO_tab(i).MAKE_ID:='0';
      end if;        
      v_trac_etl_rec.track_last_val := DM_VIOL_TX_EVENT_INFO_tab(i).ETC_ACCOUNT_ID;
    end loop;
    
    /*ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_VIOL_TX_EVENT_INFO_tab.first .. DM_VIOL_TX_EVENT_INFO_tab.last
           INSERT INTO DM_VIOL_TX_EVENT_INFO VALUES DM_VIOL_TX_EVENT_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    v_trac_etl_rec.end_val := v_trac_etl_rec.track_last_val;
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

grant execute on DM_VIOL_TX_EVENT_INFO_PROC to public;
commit;


