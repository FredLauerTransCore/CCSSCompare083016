/********************************************************
*
* Name: DM_VIOL_TX_MTCH1_INFO_PROC
* Created by: RH, 5/24/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_VIOL_TX_MTCH1_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_VIOL_TX_MTCH1_INFO_PROC IS

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

CURSOR C1 IS SELECT 
    TXN_ID TX_EXTERN_REF_NO
    ,EXT_MSG_SEQ_NUM TX_SEQ_NUMBER
    ,TOUR_TOUR_SEQ EXTERN_FILE_ID
    ,EXT_LANE_ID LANE_ID
    ,EXT_DATE_TIME TX_TIMESTAMP
    ,0 TX_MOD_SEQ
    ,'V' TX_TYPE_IND
    ,'F' TX_SUBTYPE_IND
    ,DECODE(substr(pp.PLAZA_ID,1,3),'004', 'C', 'B') TOLL_SYSTEM_TYPE  -- Derived IF PLAZA_ID STARTS WITH '004' THEN 'C' ELSE 'B'
    ,EXT_LANE_TYPE_CODE LANE_MODE
    ,1 LANE_TYPE
    ,0 LANE_STATE
    ,0 LANE_HEALTH
    ,(select AGENCY_ID from ST_INTEROP_AGENCIES where ENT_PLAZA_ID = pp.PLAZA_ID)  PLAZA_AGENCY_ID 
    ,EXT_PLAZA_ID PLAZA_ID
    ,0 COLLECTOR_ID
    ,0 TOUR_SEGMENT_ID
    ,0 ENTRY_DATA_SOURCE
    ,ENT_LANE_ID ENTRY_LANE_ID
    ,ENT_PLAZA_ID ENTRY_PLAZA_ID
    ,ENT_DATE_TIME ENTRY_TIMESTAMP
    ,TXN_ENTRY_NUMBER ENTRY_TX_SEQ_NUMBER
    ,0 ENTRY_VEHICLE_SPEED
    ,0 LANE_TX_STATUS
    ,0 LANE_TX_TYPE
    ,0 TOLL_REVENUE_TYPE
    ,AVC_CLASS ACTUAL_CLASS
    ,AVC_CLASS ACTUAL_AXLES
    ,0 ACTUAL_EXTRA_AXLES
    ,0 COLLECTOR_CLASS
    ,0 COLLECTOR_AXLES
    ,0 PRECLASS_CLASS
    ,0 PRECLASS_AXLES
    ,0 POSTCLASS_CLASS
    ,0 POSTCLASS_AXLES
    ,0 FORWARD_AXLES
    ,0 REVERSE_AXLES
    ,TOLL_AMT_FULL FULL_FARE_AMOUNT
    ,TOLL_AMT_CHARGED DISCOUNTED_AMOUNT
    ,TOLL_AMT_COLLECTED COLLECTED_AMOUNT
    ,0 UNREALIZED_AMOUNT
    ,'N' IS_DISCOUNTABLE
    ,'N' IS_MEDIAN_FARE
    ,'N' IS_PEAK
    ,0 PRICE_SCHEDULE_ID
    ,0 VEHICLE_SPEED
    ,0 RECEIPT_ISSUED
    
-- JOIN WITH JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN AND 
-- JOIN UFM_TOKEN TO UFM_ID FOR EVENT_ROV    
    ,(select HOST from PA_ACCT where ACCT_NUM=lt.ACCT_NUM)   DEVICE_NO

    ,(select ACCTTYPE_ACCT_TYPE_CODE from PA_ACCT where ACCT_NUM=lt.ACCT_NUM)  ACCOUNT_TYPE -- PA_ACCT
    ,0 DEVICE_CODED_CLASS
    ,0 DEVICE_AGENCY_CLASS
    ,0 DEVICE_IAG_CLASS
    ,0 DEVICE_AXLES
    ,kl.ACCT_ID ETC_ACCOUNT_ID  -- KS_LEDGER

-- ACCOUNT_AGENCY_ID - SUBSTR(TRANSP_ID,9,2) JOIN ST_INTEROP_AGENCIES ON AGENCY_CODE RETURN AGENCY_ID [XEROX TO LOOKUP INTERNAL ID]
    ,SUBSTR(TRANSP_ID,9,2)  ACCOUNT_AGENCY_ID 

    ,0 READ_AVI_CLASS
    ,0 READ_AVI_AXLES
    ,N' DEVICE_PROGRAM_STATUS
    ,N' BUFFERED_READ_FLAG
    ,0 LANE_DEVICE_STATUS
    ,0 POST_DEVICE_STATUS
    ,0 PRE_TXN_BALANCE
    ,1 PLAN_TYPE_ID
    ,0 ETC_TX_STATUS
    ,'F' SPEED_VIOL_FLAG
    ,'Y' IMAGE_TAKEN

 -- LOOKUP PA_STATE_CODE. Xerox - internal lookup in DM_ADDRESS_INFO and assign country correctly. 
 -- visit DM_VEHICLE_INFO and DM_ACCT_INFO
    ,NULL PLATE_COUNTRY
    ,STATE_ID_CODE PLATE_STATE -- JOIN TO PA_STATE_CODE RETURN STATE_CODE_ABBR

    ,VEH_LIC_NUM PLATE_NUMBER
    ,trunc(EXT_DATE_TIME) REVENUE_DATE
    ,trunc(TXN_PROCESS_DATE) POSTED_DATE
    ,0 ATP_FILE_ID
    ,TXN_PROCESS_DATE UPDATE_TS
    ,'N' IS_REVERSED
    ,0 CORR_REASON_ID
    ,trunc(TXN_PROCESS_DATE) RECON_DATE
    ,0 RECON_STATUS_IND
    ,0 RECON_SUB_CODE_IND
    ,to_date('01-01-1900','MM-DD-YYYY') EXTERN_FILE_DATE
    ,0 MILEAGE
    ,0 INTERNAL_AUDIT_ID
    ,0 MODIFIED_STATUS
    ,0 EXCEPTION_STATUS
    ,'000000000000' DEPOSIT_ID

-- JOIN FROM PA_LANE_TXN REQUEST_ID to EVENT_LOOKUP_ROV ROV_ID to RETURN REQUEST_DATE
    ,(select REQUEST_DATE from EVENT_LOOKUP_ROV where ROV_ID = lt.REQUEST_ID) LOAD_DATE 

    ,3 VIOL_TYPE
    ,0 REVIEWED_VEHICLE_TYPE
    ,0 REVIEWED_CLASS
    ,0 IMAGE_RVW_CLERK_ID
    ,0 IMAGE_BATCH_ID
    ,0 IMAGE_BATCH_SEQ_NUMBER
    ,VPS_EVENT_ID IMAGE_INDEX
    ,0 ADJUSTED_AMOUNT
    
-- AMT_CHARGED - TOTAL_AMT_PAID JOIN WITH KS_LEDGER ON LEDGER_ID 
--  AND KS_LEDGER TO PA_LANE_TXN on PA_LANE_TXN_ID
    ,NULL NOTICE_TOLL_AMOUNT
    
    ,0 OUTPUT_FILE_ID
    ,'**' OUTPUT_FILE_TYPE
    ,decode(SUBSTR(TRANSP_ID,9,2),'20','N','Y') IS_RECIPROCITY_TXN  -- IF SUBSTR(TRANSP_ID,9,2) = '20' THEN NO ELSE YES 
    ,0 MAKE_ID
    
    ,NULL EVENT_TIMESTAMP -- EXT_DATE_TIME Default PA_LANE_TXN

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
--JOIN ID of KS_LEDGER to LEDGER_ID of VB_ACTVITY for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID of PA_LANE_LANE_TXN and JOIN PLAZA_ID of PA_PLAZA to EXT_PLAZA_ID for PA_Lane_txn
--  IF SUM (VB_ACTIVITY.AMT_CHARGED � VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS grouped by KS_LEDGER.ACCT_NUM
--  FOR PA_PLAZA.FTE_PLAZA = 'Y',VB_ACTIVITY.COLL_COURT_FLAG is NULL, VB_ACTIVITY.CHILD_DOC_ID not null,VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and VB_ACTIVITY.BANKRUPTCY_flag is null
--  THEN 'REG STOP'    
    ,NULL EVENT_TYPE

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
    ,TXN_PROCESS_DATE IMAGE_REVIEW_TIMESTAMP
    ,trunc(TXN_PROCESS_DATE) REVIEWED_DATE
    ,trunc(EXT_DATE_TIME) TX_DATE
    ,'NA' CITATION_LEVEL
    ,'NA' CITATION_STATUS

 --TOTAL_AMT_PAID JOIN WITH KS_LEDGER ON LEDGER_ID AND KS_LEDGER TO PA_LANE_TXN on PA_LANE_TXN_ID
    ,NULL AMOUNT_PAID

    ,decode(VIOL_TX_STATUS,'UTC',25,0) NOTICE_FEE_AMOUNT -- IF VIOL_TX_STATUS = 'UTC' then $25 ELSE 0

-- {IF AMT_CHARGED = TOTAL_AMT_PAID } THEN 'Y' ELSE 
-- JOIN EXT_ACTIVITY_PAID JOIN WITH KS_LEDGER ON LEDGER_ID 
-- AND KS_LEDGER TO PA_LANE_TXN on PA_LANE_TXN_ID 
-- (IF NOT EXISTS, THEN 'N')
    ,NULL IS_CLOSED 
    ,CREATED_ON DMV_RETURN_DATE -- REFER TO EXTRACT RULE ABOVE  -EVENT_OWNER_ADDR_VEHICLE_ACCT
    
--PAID_ON_COLLECTION or SEND_TO_COLLECTION;  
--  IF VB_ACTIVITY.COLL_COURT_FLAG = 'COLL' THEN 'SEND_TO_COLLECTION' 
--  IF ST_ACTIVITY_PAID.COLL_COURT_FLAG = 'COLL'  AND AMT_CHARGED <> 0 THEN PAID_ON_COLLECTIONS    
    ,NULL COLL_STATUS
    
    ,0 LIMOUSINE
    ,0 TAXI
    ,0 TAXI_LIMO 
    ,(select ACCT_NUM from PA_RENTAL_AGNCY where ACCT_NUM = pp.ACCT_NUM)  RENTAL_COMPANY_ID -- PA_RENTAL_AGNCY
    ,NVL2(OAVA_LINK_ID,'DMV','OTH') ADDRESS_SOURCE  -- IF OAVA_LINK_ID IS NOT NULL THEN 'DMV' ELSE 'OTH'
    ,trunc(EXT_DATE_TIME) IMAGE_RECEIVE_DATE
    ,0 IMG_FILE_INDEX
    ,'**' BASEFILENAME -- ** Vector assigns internally **NOTE**
    ,pp.PLAZA_NAME LOCATION  -- JOIN PA_PLAZA ON PLAZA_ID RETURN PLAZA_NAME
    
    ,NULL DMV_MAKE_ID -- EVENT_VEHICLE - REFER TO EXTRACT RULE ABOVE (#2)
    
    ,0 PAR_LANE_TX_ID 
    ,pp.FACCODE_FACILITY_CODE FACILITY_ID -- JOIN PA_PLAZA ON PLAZA_ID RETURN FACCODE_FACILITY_CODE
    ,0 FARE_TBL_ID
    ,lt.EXT_MSG_SEQ_NUM LANE_SEQ_NO
    ,decode(kl.TRANSACTION_TYPE,1,0)  TXN_DISPUTED  -- IF TXN_TYPE = 253 THEN 1 ELSE 0 (only for iTolls)
    ,substr(kl.DESCRIPTION,41,8) DISPUTED_ETC_ACCT_ID  -- substr(a.description,41,8) on KS_LEDGER joining PA_LANE_TXN on pa_lane_txn_id
FROM PATRON.PA_LANE_TXN lt
    ,PATRON.KS_LEDGER kl
    ,PATRON.PA_PLAZA pp
WHERE lt.txn_id = kl.PA_LANE_TXN_ID
AND lt.EXT_PLAZA_ID = pp.PLAZA_ID (+)
AND lt.TRANSP_ID like '%2010'  --WHERE TRANSPONDER_ID ENDS WITH '2010'  
;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VIOL_TX_MTCH1_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_VIOL_TX_MTCH1_INFO_tab.first .. DM_VIOL_TX_MTCH1_INFO_tab.last
           INSERT INTO DM_VIOL_TX_MTCH1_INFO VALUES DM_VIOL_TX_MTCH1_INFO_tab(i);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;

  COMMIT;

  CLOSE C1;

  COMMIT;

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
END;
/
SHOW ERRORS


