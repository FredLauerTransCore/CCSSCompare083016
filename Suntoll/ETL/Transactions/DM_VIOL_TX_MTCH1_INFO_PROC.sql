/********************************************************
*
* Name: DM_VIOL_TX_MTCH1_INFO_PROC
* Created by: RH, 5/24/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_VIOL_TX_MTCH1_INFO
*
********************************************************/


---No Development action provided  165, 576 and 579

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_VIOL_TX_MTCH1_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

v_date_range NUMBER := 1;
v_end_date    DATE  := trunc(SYSDATE)-100;
v_begin_date  DATE  := v_end_date-v_date_range;

TYPE DM_VIOL_TX_MTCH1_INFO_TYP IS TABLE OF DM_VIOL_TX_MTCH1_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_VIOL_TX_MTCH1_INFO_tab DM_VIOL_TX_MTCH1_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;

-- EXTRACT RULE = SELECT * FROM PA_LANE_TXN WHERE TRANSPONDER_ID ENDS WITH '2010'  
/* indicates a violation toll */ -- (rentals included) AND IF NON '2010' INDICATES AN AGENCY_TOLL 

-- EXTRACT RULE FOR ROV = 
-- Join OAVA_LINK_ID of PA_LANE_TXN to ID of EVENT_OWNER_ADDR_VEHICLE_ACCT 
--  AND OWNER_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_OWNER 
--  AND ADDRESS_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_ADDRESS 
--  AND VEHICLE_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_VEHICLE

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    lt.TXN_ID TX_EXTERN_REF_NO   -- Primary key
    ,nvl(lt.EXT_MSG_SEQ_NUM,0) TX_SEQ_NUMBER
    ,lt.TOUR_TOUR_SEQ EXTERN_FILE_ID
--    ,to_number(lt.EXT_LANE_ID) LANE_ID
    ,lt.EXT_LANE_ID LANE_ID
    ,lt.EXT_DATE_TIME TX_TIMESTAMP
    ,0 TX_MOD_SEQ
    ,'V' TX_TYPE_IND
    ,'F' TX_SUBTYPE_IND
-- Derived IF PLAZA_ID STARTS WITH '004' THEN 'C' ELSE 'B'
    ,DECODE(substr(trim(lt.EXT_PLAZA_ID),1,3),'004', 'C', 'B') TOLL_SYSTEM_TYPE 
--    ,lt.EXT_LANE_TYPE_CODE LANE_MODE  -- Not a number
    ,EXT_LANE_TYPE_CODE LANE_MODE  -- Not a number
    ,1 LANE_TYPE
    ,0 LANE_STATE
    ,0 LANE_HEALTH
    ,'0' PLAZA_AGENCY_ID   -- in ETL
        
----    ,to_number(nvl(lt.EXT_PLAZA_ID,'0')) PLAZA_ID
    ,to_number(lt.EXT_PLAZA_ID) PLAZA_ID
    ,0 COLLECTOR_ID
    ,0 TOUR_SEGMENT_ID
    ,'0' ENTRY_DATA_SOURCE
    ,to_number(nvl(lt.ENT_LANE_ID,'0')) ENTRY_LANE_ID  -- Required - If NULL default ?
    ,to_number(nvl(lt.ENT_PLAZA_ID,'0')) ENTRY_PLAZA_ID  -- Required - If NULL default ?
    ,nvl(lt.ENT_DATE_TIME, to_date('01-01-1900','MM-DD-YYYY')) ENTRY_TIMESTAMP  -- Required - If NULL default ?
    ,nvl(lt.TXN_ENTRY_NUMBER,0) ENTRY_TX_SEQ_NUMBER  -- Required - If NULL default ?
    ,0 ENTRY_VEHICLE_SPEED
    ,0 LANE_TX_STATUS
    ,0 LANE_TX_TYPE
    ,0 TOLL_REVENUE_TYPE
    ,to_number(lt.AVC_CLASS) ACTUAL_CLASS
    ,to_number(lt.AVC_CLASS) ACTUAL_AXLES
    ,0 ACTUAL_EXTRA_AXLES
    ,0 COLLECTOR_CLASS
    ,0 COLLECTOR_AXLES
    ,0 PRECLASS_CLASS
    ,0 PRECLASS_AXLES
    ,0 POSTCLASS_CLASS
    ,0 POSTCLASS_AXLES
    ,0 FORWARD_AXLES
    ,0 REVERSE_AXLES
    ,nvl(lt.TOLL_AMT_FULL,0) FULL_FARE_AMOUNT
    ,nvl(lt.TOLL_AMT_CHARGED,0) DISCOUNTED_AMOUNT
    ,nvl(lt.TOLL_AMT_COLLECTED,0) COLLECTED_AMOUNT
    ,0 UNREALIZED_AMOUNT
    ,'N' IS_DISCOUNTABLE
    ,'N' IS_MEDIAN_FARE
    ,'N' IS_PEAK
    ,0 PRICE_SCHEDULE_ID
    ,0 VEHICLE_SPEED
    ,0 RECEIPT_ISSUED

---- JOIN WITH JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN AND 
---- JOIN UFM_TOKEN TO UFM_ID FOR EVENT_ROV       
---- -    ,(select HOST_UFM_TOKEN from UFM_LANE_TXN_INFO where TXN_ID=lt.TXN_ID)   HOST_UFM_TOKEN
    ,'***'   DEVICE_NO  -- in ETL

    ,NULL  ACCOUNT_TYPE -- PA_ACCT  -- KS_LEDGER  ---- in ETL
    ,0 DEVICE_CODED_CLASS
    ,0 DEVICE_AGENCY_CLASS
    ,0 DEVICE_IAG_CLASS
    ,0 DEVICE_AXLES
    ,0 ETC_ACCOUNT_ID  -- KS_LEDGER  ---- in ETL

---------- ACCOUNT_AGENCY_ID - SUBSTR(TRANSP_ID,9,2) 
--SUBSTR(TRANSP_ID,9,2) JOIN ST_INTEROP_AGENCIES ON AGENCY_CODE 
--RETURN AGENCY_ID [XEROX TO LOOKUP INTERNAL ID]
--Only bring in two digit location. Lookup will occur after staging by Xerox.
    ,SUBSTR(lt.TRANSP_ID,9,2) ACCOUNT_AGENCY_ID  -- Use to get in ETL
        
    ,0 READ_AVI_CLASS
    ,0 READ_AVI_AXLES
    ,'N' DEVICE_PROGRAM_STATUS
    ,'N' BUFFERED_READ_FLAG
    ,to_number(nvl(lt.MSG_INVALID,'0')) LANE_DEVICE_STATUS  -- Required - If NULL default ?
    ,0 POST_DEVICE_STATUS
    ,0 PRE_TXN_BALANCE
    ,1 PLAN_TYPE_ID
    ,0 ETC_TX_STATUS
    ,'F' SPEED_VIOL_FLAG
    ,'Y' IMAGE_TAKEN

--LOOKUP PA_STATE_CODE. Xerox - internal lookup in DM_ADDRESS_INFO and assign country correctly.  
    ,'USA' PLATE_COUNTRY -- in ETL
    ,lt.STATE_ID_CODE PLATE_STATE  -- JOIN TO PA_STATE_CODE RETURN STATE_CODE_ABBR
    ,lt.VEH_LIC_NUM PLATE_NUMBER
    ,trunc(lt.EXT_DATE_TIME) REVENUE_DATE
    ,trunc(lt.TXN_PROCESS_DATE) POSTED_DATE
    ,0 ATP_FILE_ID
    ,lt.TXN_PROCESS_DATE UPDATE_TS
    ,'N' IS_REVERSED
    ,0 CORR_REASON_ID
    ,trunc(lt.TXN_PROCESS_DATE) RECON_DATE
    ,0 RECON_STATUS_IND
    ,0 RECON_SUB_CODE_IND
    ,to_date('01-01-1900','MM-DD-YYYY') EXTERN_FILE_DATE
    ,0 MILEAGE
    ,0 INTERNAL_AUDIT_ID
    ,0 MODIFIED_STATUS
    ,0 EXCEPTION_STATUS
    ,'000000000000' DEPOSIT_ID

-------- JOIN FROM PA_LANE_TXN REQUEST_ID to EVENT_LOOKUP_ROV ROV_ID to RETURN REQUEST_DATE
--    ,nvl((select elr.REQUEST_DATE 
--        from EVENT_LOOKUP_ROV elr
--        where elr.EVENT_OAVA_LINK_ID=lt.OAVA_LINK_ID  --        and   rownum=1
--        ),SYSDATE) LOAD_DATE
------    ,(select elr.REQUEST_DATE from EVENT_LOOKUP_ROV elr
------        where elr.EVENT_OAVA_LINK_ID=lt.OAVA_LINK_ID  --        and   rownum=1
------        ) LOAD_DATE
------    ,(select elr.REQUEST_DATE from EVENT_LOOKUP_ROV elr 
------        where elr.ID = lt.VPS_EVENT_ID) LOAD_DATE
------    ,(select elr.REQUEST_DATE
------        from EVENT_LOOKUP_ROV elr, EVENT_ROV er
------        where elr.id = er.LOOKUP_Q_ID
------        and   elr.EVENT_OAVA_LINK_ID = lt.OAVA_LINK_ID) LOAD_DATE
    ,SYSDATE LOAD_DATE 
    
    ,3 VIOL_TYPE
    ,0 REVIEWED_VEHICLE_TYPE
    ,0 REVIEWED_CLASS
    ,0 IMAGE_RVW_CLERK_ID
    ,0 IMAGE_BATCH_ID
    ,0 IMAGE_BATCH_SEQ_NUMBER
    
--    ,to_number(nvl(lt.VPS_EVENT_ID,'0')) IMAGE_INDEX  -- Source is NUMBER(20) and target is NUMBER(5)
--    ,to_number(substr(nvl(lt.VPS_EVENT_ID,'0'),1,5)) IMAGE_INDEX  
    ,to_number(substr(lt.VPS_EVENT_ID,1,5)) IMAGE_INDEX  -- check for null

    ,0 ADJUSTED_AMOUNT
-- AMT_CHARGED - TOTAL_AMT_PAID JOIN WITH KS_LEDGER ON LEDGER_ID 
--  AND KS_LEDGER TO PA_LANE_TXN on PA_LANE_TXN_ID
    ,(nvl(lt.TOLL_AMT_CHARGED,0)-nvl(lt.TOLL_AMT_COLLECTED,0)) NOTICE_TOLL_AMOUNT   
    ,0 OUTPUT_FILE_ID
    ,'**' OUTPUT_FILE_TYPE
    
    ,CASE WHEN SUBSTR(TRANSP_ID,9,2) = '20' 
          THEN 'N'
          ELSE 'Y'
      END IS_RECIPROCITY_TXN  -- IF SUBSTR(TRANSP_ID,9,2) = '20' THEN NO ELSE YES 
    ,0 MAKE_ID
    ,lt.EXT_DATE_TIME EVENT_TIMESTAMP -- EXT_DATE_TIME Default PA_LANE_TXN
    ,'UNDEFINED' EVENT_TYPE -- Derived   -- VB_ACTIVITY (IN ETL)

    ,0 PREV_EVENT_TYPE
    ,0 VIOL_TX_STATUS
    ,0 PREV_VIOL_TX_STATUS
    ,0 DMV_PLATE_TYPE
    ,0 RECON_PLAN_ID
    ,to_date('01-01-1900','MM-DD-YYYY') RECON_TIMESTAMP
    ,0 DEVICE_READ_COUNT
    ,0 DEVICE_WRITE_COUNT
    ,0 ENTRY_DEVICE_READ_COUNT
    ,0 ENTRY_DEVICE_WRITE_COUNT
    ,0 REVIEWED_SEGMENT_ID
    ,lt.TXN_PROCESS_DATE IMAGE_REVIEW_TIMESTAMP
    ,trunc(lt.TXN_PROCESS_DATE) REVIEWED_DATE
    ,trunc(lt.EXT_DATE_TIME) TX_DATE
    ,0 CITATION_LEVEL --'NA'
    ,0 CITATION_STATUS --'NA'
--TOTAL_AMT_PAID JOIN WITH KS_LEDGER ON LEDGER_ID AND 
 -- KS_LEDGER TO PA_LANE_TXN on PA_LANE_TXN_ID
    ,nvl(lt.TOLL_AMT_COLLECTED,0) AMOUNT_PAID
    
-- -- IF VIOL_TX_STATUS = 'UTC' then $25 ELSE 0  -- Derived  
    ,0 NOTICE_FEE_AMOUNT  -- Derived   -- VB_ACTIVITY (IN ETL)
      
---- {IF AMT_CHARGED = TOTAL_AMT_PAID } THEN 'Y' ELSE 
---- JOIN EXT_ACTIVITY_PAID JOIN WITH KS_LEDGER ON LEDGER_ID 
---- AND KS_LEDGER TO PA_LANE_TXN on PA_LANE_TXN_ID -- (IF NOT EXISTS, THEN 'N')
    ,CASE WHEN lt.TOLL_AMT_CHARGED=lt.TOLL_AMT_COLLECTED 
          THEN 'Y'
          ELSE 'N'
      END IS_CLOSED 
      
------  EXTRACT RULE FOR ROV = 
------  Join OAVA_LINK_ID of PA_LANE_TXN to ID of EVENT_OWNER_ADDR_VEHICLE_ACCT 
------  AND OWNER_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_OWNER 
------  AND ADDRESS_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_ADDRESS 
------  AND VEHICLE_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_VEHICLE
     ,to_date('12-31-9999','MM-DD-YYYY') DMV_RETURN_DATE
        
----  IF VB_ACTIVITY.COLL_COURT_FLAG = 'COLL' THEN 'SEND_TO_COLLECTION' 
----  IF ST_ACTIVITY_PAID.COLL_COURT_FLAG = 'COLL'  AND AMT_CHARGED <> 0 THEN PAID_ON_COLLECTIONS    
    ,0 COLL_STATUS  -- ST_ACTIVITY_PAID and VB_ACTIVITY (IN ETL)
    
    ,0 LIMOUSINE
    ,0 TAXI
    ,0 TAXI_LIMO 
    ,'UNDEFINED' RENTAL_COMPANY_ID -- PA_RENTAL_AGNCY -- NULLs (IN ETL)
        
    ,NVL2(lt.OAVA_LINK_ID,'DMV','OTH') ADDRESS_SOURCE  -- IF OAVA_LINK_ID IS NOT NULL THEN 'DMV' ELSE 'OTH'
    ,trunc(lt.EXT_DATE_TIME) IMAGE_RECEIVE_DATE  
    ,'0' IMG_FILE_INDEX
    ,'**' BASEFILENAME -- ** Vector assigns internally **NOTE** 
    ,'UNDEFINED'  LOCATION  --   -- in ETL section  ,lt.EXT_PLAZA_ID LOCATION
--        
---- -- EVENT_VEHICLE - REFER TO EXTRACT RULE ABOVE (#2)
    ,lt.OAVA_LINK_ID DMV_MAKE_ID  -- - Use to get in ETL 

    ,0 PAR_LANE_TX_ID 
    ,'UNDEFINED' FACILITY_ID  -- in ETL section

    ,0 FARE_TBL_ID
    ,lt.EXT_MSG_SEQ_NUM LANE_SEQ_NO
-- IF TXN_TYPE = 253 THEN 1 ELSE 0 (only for iTolls)
    ,0 TXN_DISPUTED          -- KS_LEDGER (IN ETL)
    ,0 DISPUTED_ETC_ACCT_ID  -- KS_LEDGER (IN ETL)
    ,EXT_DATE_TIME EXT_DATE_TIME
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_LANE_TXN lt
where lt.txn_id = (select PA_LANE_TXN_ID
                from   KS_LEDGER kl2
                WHERE kl2.PA_LANE_TXN_ID = lt.txn_id
                AND   kl2.ACCT_NUM >= p_begin_acct_num AND   kl2.ACCT_NUM <= p_end_acct_num
                and   rownum<=1
                )
and lt.TRANSP_ID like '%2010'  --WHERE TRANSPONDER_ID ENDS WITH '2010'  
;

v_ks_ledger_id    KS_LEDGER.ID%TYPE:= 0;
v_acct_num        KS_LEDGER.ACCT_NUM%TYPE:= 0;
va_rec            VB_ACTIVITY%ROWTYPE;

row_cnt           NUMBER := 0;
v_trac_rec        dm_tracking%ROWTYPE;
v_trac_etl_rec    dm_tracking_etl%ROWTYPE;

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
    FETCH C1 BULK COLLECT INTO DM_VIOL_TX_MTCH1_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN*/
    
    FOR i IN 1 .. DM_VIOL_TX_MTCH1_INFO_tab.COUNT LOOP
      begin
        select  ID
               ,ACCT_NUM
               ,decode(TRANSACTION_TYPE, 253, 1, 0)
               ,decode(TRANSACTION_TYPE, 
                  253, TO_NUMBER(trim(translate(substr(description,40,9),'-PT','   '))), 0)
        into    v_ks_ledger_id
               ,DM_VIOL_TX_MTCH1_INFO_tab(i).ETC_ACCOUNT_ID
               ,DM_VIOL_TX_MTCH1_INFO_tab(i).TXN_DISPUTED
               ,DM_VIOL_TX_MTCH1_INFO_tab(i).DISPUTED_ETC_ACCT_ID
        from    KS_LEDGER -- k1
        where   PA_LANE_TXN_ID=DM_VIOL_TX_MTCH1_INFO_tab(i).TX_EXTERN_REF_NO
--        and     rownum<=1
        and     POSTED_DATE = (select max(POSTED_DATE) 
                              from    KS_LEDGER -- k2
                              where   PA_LANE_TXN_ID=DM_VIOL_TX_MTCH1_INFO_tab(i).TX_EXTERN_REF_NO)
--                              where k2.id = k1.id)
        ;
      exception 
        when others then null;
        DM_VIOL_TX_MTCH1_INFO_tab(i).TXN_DISPUTED := 0;
        DM_VIOL_TX_MTCH1_INFO_tab(i).DISPUTED_ETC_ACCT_ID := 0;
      end;
      
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_VIOL_TX_MTCH1_INFO_tab(i).ETC_ACCOUNT_ID;
      end if;
      
--select agency_id  --  PLAZA_AGENCY_ID
--from PA_LANE_TXN a, PA_PLAZA b, ST_INTEROP_AGENCIES c
--Where a.ext_plaza_id  = b.plaza_id
--And AUTHCODE_AUTHORITY_CODE = AUTHORITY_CODE
-- Agency  id to which the plaza and lane belong to where the transaction took place.
      begin
        select sia.AGENCY_ID 
        into  DM_VIOL_TX_MTCH1_INFO_tab(i).PLAZA_AGENCY_ID 
        from  PA_PLAZA pp, ST_INTEROP_AGENCIES sia
        Where pp.plaza_id  = DM_VIOL_TX_MTCH1_INFO_tab(i).PLAZA_ID
        and   pp.AUTHCODE_AUTHORITY_CODE = sia.AUTHORITY_CODE
        and   rownum<=1
        ;
      exception
        when no_data_found then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).PLAZA_AGENCY_ID:='0'; 
        when others then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).PLAZA_AGENCY_ID:='0'; 
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
           DBMS_OUTPUT.PUT_LINE('PLAZA_AGENCY_ID ERROR CODE: '||v_trac_etl_rec.result_code);
           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;

---- JOIN WITH JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN AND 
---- JOIN UFM_TOKEN TO UFM_ID FOR EVENT_ROV       
---- -    ,(select HOST_UFM_TOKEN from UFM_LANE_TXN_INFO where TXN_ID=lt.TXN_ID)   HOST_UFM_TOKEN
      begin
        select nvl(er.TRANSP_ID, '***')
        into  DM_VIOL_TX_MTCH1_INFO_tab(i).DEVICE_NO
        from  EVENT_ROV er, 
              UFM_LANE_TXN_INFO ult
        where er.UFM_ID = ult.HOST_UFM_TOKEN 
        and   ult.TXN_ID = DM_VIOL_TX_MTCH1_INFO_tab(i).TX_EXTERN_REF_NO   
        and   rownum<=1
        ;
      exception
        when others then null;
        DM_VIOL_TX_MTCH1_INFO_tab(i).DEVICE_NO := '***';
      end;
      
------------ ACCOUNT_AGENCY_ID - SUBSTR(TRANSP_ID,9,2) 
----SUBSTR(TRANSP_ID,9,2) JOIN ST_INTEROP_AGENCIES ON AGENCY_CODE 
----RETURN AGENCY_ID [XEROX TO LOOKUP INTERNAL ID]
----Only bring in two digit location. Lookup will occur after staging by Xerox.
--      begin
--        select distinct AGENCY_ID 
--        into  DM_VIOL_TX_MTCH1_INFO_tab(i).ACCOUNT_AGENCY_ID 
--        from  ST_INTEROP_AGENCIES sia
--        Where AGENCY_CODE  = DM_VIOL_TX_MTCH1_INFO_tab(i).ACCOUNT_AGENCY_ID
--        -- and   rownum<=1
--        ;
--      exception
--        when no_data_found then null;
--          DM_VIOL_TX_MTCH1_INFO_tab(i).PLAZA_AGENCY_ID:='0'; 
--        when others then null;
--          DM_VIOL_TX_MTCH1_INFO_tab(i).ACCOUNT_AGENCY_ID:='0'; 
--           DBMS_OUTPUT.PUT_LINE('AGENCY_CODE: '||DM_VIOL_TX_MTCH1_INFO_tab(i).ACCOUNT_AGENCY_ID);
--          v_trac_etl_rec.result_code := SQLCODE;
--          v_trac_etl_rec.result_msg := SQLERRM;
--          v_trac_etl_rec.proc_end_date := SYSDATE;
--          update_track_proc(v_trac_etl_rec);
--           DBMS_OUTPUT.PUT_LINE('ACCOUNT_AGENCY_ID ERROR CODE: '||v_trac_etl_rec.result_code);
--           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
--      end;
      
--LOOKUP PA_STATE_CODE. Xerox - internal lookup in DM_ADDRESS_INFO and assign country correctly. 
      begin
        select COUNTRY into DM_VIOL_TX_MTCH1_INFO_tab(i).PLATE_COUNTRY 
        from  COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_VIOL_TX_MTCH1_INFO_tab(i).PLATE_STATE;
      exception
        when others then null;
        DM_VIOL_TX_MTCH1_INFO_tab(i).PLATE_COUNTRY:='USA';
      end;

--------  ACCT_NUM in (select ACCT_NUM from PATRON.PA_RENTAL_AGENCY)
      begin
        select ACCT_NUM 
        into  DM_VIOL_TX_MTCH1_INFO_tab(i).RENTAL_COMPANY_ID 
        from  PA_RENTAL_AGENCY 
        where ACCT_NUM = DM_VIOL_TX_MTCH1_INFO_tab(i).ETC_ACCOUNT_ID;
      exception
        when others then null;
        DM_VIOL_TX_MTCH1_INFO_tab(i).RENTAL_COMPANY_ID:='UNDEFINED';
      end;
        
      begin
        select ACCTTYPE_ACCT_TYPE_CODE 
        into  DM_VIOL_TX_MTCH1_INFO_tab(i).ACCOUNT_TYPE 
        from  PA_ACCT 
        where ACCT_NUM = DM_VIOL_TX_MTCH1_INFO_tab(i).ETC_ACCOUNT_ID;
      exception
        when others then null;
        DM_VIOL_TX_MTCH1_INFO_tab(i).ACCOUNT_TYPE:=NULL;
      end;        
        
        
--VB_ACTIVITY table 
--FOR BANKRUPTCY_FLAG NULL and COLL_COURT_FLAG NULL
--IF DOCUMENT_ID is null, then 'UNBILLED'
--IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NULL, then 'INVOICED'
--IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL, then 'ESCALATED'
--IF DOCUMENT_ID is not null and CHILD_DOC_ID like '%-%', then 'UTC'
--FOR BANKRUPTCY_FLAG NULL and COLL_COURT_FLAG NOT NULL
--IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL  and COLL_COURT_FLAG is 'COLL' THEN 'COLLECTION'
--IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL  and COLL_COURT_FLAG is 'CRT' THEN 'COURT'
--
--FOR BANKRUPTCY _FLAG NOT NULL
--IF BANKRUPTCY _FLAG is not null, then 'BANKRUPTCY'
--JOIN ID of KS_LEDGER to LEDGER_ID of VB_ACTVITY 
--    for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
--JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID of PA_LANE_LANE_TXN and 
--JOIN PLAZA_ID of PA_PLAZA to EXT_PLAZA_ID 
--    for PA_Lane_txn
--  IF SUM (VB_ACTIVITY.AMT_CHARGED � VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS grouped by KS_LEDGER.ACCT_NUM
--  FOR PA_PLAZA.FTE_PLAZA = 'Y',
--    VB_ACTIVITY.COLL_COURT_FLAG is NULL, 
--    VB_ACTIVITY.CHILD_DOC_ID not null,
--    VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and 
--    VB_ACTIVITY.BANKRUPTCY_flag is null
--  THEN 'REG STOP'
  
      begin
        select CASE WHEN BANKRUPTCY_FLAG is NOT null THEN 'BANKRUPTCY'
          WHEN COLL_COURT_FLAG is null  and CHILD_DOC_ID is null     THEN 'INVOICED'
          WHEN COLL_COURT_FLAG is null  and CHILD_DOC_ID like '%-%'  THEN 'UTC'
          WHEN COLL_COURT_FLAG is null  and CHILD_DOC_ID is NOT null THEN 'ESCALATED'
          WHEN COLL_COURT_FLAG = 'COLL' and CHILD_DOC_ID is NOT null THEN 'COLLECTION'
          WHEN COLL_COURT_FLAG = 'CRT'  and CHILD_DOC_ID is NOT null THEN 'COURT'
--          WHEN BANKRUPTCY_FLAG is NULL THEN 'REG STOP' -- TODO: Need to add criteria for reg stop 
          ELSE 'UNDEFINED'
         END    
        ,CASE WHEN COLL_COURT_FLAG is null and  DOCUMENT_ID is NOT null and CHILD_DOC_ID like '%-%' 
              THEN 25
              ELSE 0
         END 
        ,CASE WHEN COLL_COURT_FLAG = 'COLL' THEN 1 -- 'SEND_TO_COLLECTION' 
--             WHEN ap.COLL_COURT_FLAG = 'COLL' AND ap.AMT_CHARGED <> 0 THEN 2  -- 'PAID_ON_COLLECTIONS'         
             ELSE 0
         END 
        into  DM_VIOL_TX_MTCH1_INFO_tab(i).EVENT_TYPE
              ,DM_VIOL_TX_MTCH1_INFO_tab(i).NOTICE_FEE_AMOUNT
              ,DM_VIOL_TX_MTCH1_INFO_tab(i).COLL_STATUS
        from  VB_ACTIVITY
        where LEDGER_ID = v_ks_ledger_id
        ;
      exception 
        when no_data_found then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).EVENT_TYPE := 'UNBILLED';
          DM_VIOL_TX_MTCH1_INFO_tab(i).NOTICE_FEE_AMOUNT := 0;
          DM_VIOL_TX_MTCH1_INFO_tab(i).COLL_STATUS := 0;
        when others then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).EVENT_TYPE := 'UNDEFINED';
          DM_VIOL_TX_MTCH1_INFO_tab(i).NOTICE_FEE_AMOUNT := 0;
          DM_VIOL_TX_MTCH1_INFO_tab(i).COLL_STATUS := 0;
      end;

      if DM_VIOL_TX_MTCH1_INFO_tab(i).COLL_STATUS != 1 then
        begin
          select CASE WHEN COLL_COURT_FLAG = 'COLL' AND AMT_CHARGED <> 0 THEN 2  -- 'PAID_ON_COLLECTIONS'         
               ELSE 0
           END 
          into  DM_VIOL_TX_MTCH1_INFO_tab(i).COLL_STATUS
          from  ST_ACTIVITY_PAID
          where LEDGER_ID = v_ks_ledger_id
          ;
        exception 
          when others then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).COLL_STATUS := 0;
        end;
      end if;
  
-------- JOIN FROM PA_LANE_TXN REQUEST_ID to EVENT_LOOKUP_ROV ROV_ID to RETURN REQUEST_DATE
--    ,nvl((select elr.REQUEST_DATE 
--        from EVENT_LOOKUP_ROV elr
--        where elr.EVENT_OAVA_LINK_ID=lt.OAVA_LINK_ID  --        and   rownum=1
--        ),SYSDATE) LOAD_DATE
------    ,(select elr.REQUEST_DATE from EVENT_LOOKUP_ROV elr
------        where elr.EVENT_OAVA_LINK_ID=lt.OAVA_LINK_ID  --        and   rownum=1
------        ) LOAD_DATE
------    ,(select elr.REQUEST_DATE from EVENT_LOOKUP_ROV elr 
------        where elr.ID = lt.VPS_EVENT_ID) LOAD_DATE
------    ,(select elr.REQUEST_DATE
------        from EVENT_LOOKUP_ROV elr, EVENT_ROV er
------        where elr.id = er.LOOKUP_Q_ID
------        and   elr.EVENT_OAVA_LINK_ID = lt.OAVA_LINK_ID) LOAD_DATE

      begin
        select REQUEST_DATE
        into  DM_VIOL_TX_MTCH1_INFO_tab(i).LOAD_DATE
        from  EVENT_LOOKUP_ROV
        where EVENT_OAVA_LINK_ID = DM_VIOL_TX_MTCH1_INFO_tab(i).DMV_MAKE_ID
        ;
      exception 
        when no_data_found then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).LOAD_DATE := SYSDATE;      
        when others then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).LOAD_DATE := SYSDATE;      
      end;

------  EXTRACT RULE FOR ROV = 
------  Join OAVA_LINK_ID of PA_LANE_TXN to ID of EVENT_OWNER_ADDR_VEHICLE_ACCT 
------  AND OWNER_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_OWNER 
------  AND ADDRESS_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_ADDRESS 
------  AND VEHICLE_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_VEHICLE
      begin
        select CREATED_ON
        into  DM_VIOL_TX_MTCH1_INFO_tab(i).DMV_RETURN_DATE
        from  EVENT_OWNER_ADDR_VEHICLE_ACCT
        where ID = DM_VIOL_TX_MTCH1_INFO_tab(i).DMV_MAKE_ID
        ;
      exception 
        when no_data_found then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).DMV_RETURN_DATE := to_date('12-31-9999','MM-DD-YYYY');      
        when others then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).DMV_RETURN_DATE := to_date('12-31-9999','MM-DD-YYYY');      
      end; 


---- -- EVENT_VEHICLE - REFER TO EXTRACT RULE ABOVE (#2)
      begin
        select nvl(ev.MAKE, 'UNDEFINED')
        into  DM_VIOL_TX_MTCH1_INFO_tab(i).DMV_MAKE_ID
        from  EVENT_VEHICLE ev, 
              EVENT_OWNER_ADDR_VEHICLE_ACCT eo
        where eo.ID = DM_VIOL_TX_MTCH1_INFO_tab(i).DMV_MAKE_ID
        and   eo.VEHICLE_ID = ev.ID   -- and   rownum<=1
        ;
      exception 
        when no_data_found then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).DMV_MAKE_ID := 'UNDEFINED';      
        when others then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).DMV_MAKE_ID := 'OTH ERROR';      
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
           DBMS_OUTPUT.PUT_LINE('MAKE ERROR CODE: '||v_trac_etl_rec.result_code);
           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end; 
        
      begin
        select substr(trim(PLAZA_NAME),1,15) -- Target is 15 chars
              ,nvl(FACCODE_FACILITY_CODE,'UNDEFINED')
        into  DM_VIOL_TX_MTCH1_INFO_tab(i).LOCATION
              ,DM_VIOL_TX_MTCH1_INFO_tab(i).FACILITY_ID
        from  PA_PLAZA
        where PLAZA_ID = DM_VIOL_TX_MTCH1_INFO_tab(i).PLAZA_ID
        ;
      exception 
        when others then null;
          DM_VIOL_TX_MTCH1_INFO_tab(i).LOCATION := 'UNDEFINED';
          DM_VIOL_TX_MTCH1_INFO_tab(i).FACILITY_ID := 'UNDEFINED';        
      end;  
      
      v_trac_etl_rec.track_last_val := DM_VIOL_TX_MTCH1_INFO_tab(i).ETC_ACCOUNT_ID;
--      v_trac_etl_rec.end_val := DM_VIOL_TX_MTCH1_INFO_tab(i).ETC_ACCOUNT_ID;

    END LOOP;
    /*  ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_VIOL_TX_MTCH1_INFO_tab.first .. DM_VIOL_TX_MTCH1_INFO_tab.last
           INSERT INTO DM_VIOL_TX_MTCH1_INFO VALUES DM_VIOL_TX_MTCH1_INFO_tab(i);
                       
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

commit;
