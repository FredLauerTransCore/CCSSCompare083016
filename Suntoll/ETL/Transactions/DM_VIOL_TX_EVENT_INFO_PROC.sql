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

CREATE OR REPLACE PROCEDURE DM_VIOL_TX_EVENT_INFO_PROC IS

TYPE DM_VIOL_TX_EVENT_INFO_TYP IS TABLE OF DM_VIOL_TX_EVENT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_VIOL_TX_EVENT_INFO_tab DM_VIOL_TX_EVENT_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

 /** Internal Xerox lookup;  
 -- Valid status: (xerox internal values 0100 to 0200) 
 -- FTE:  Unbilled; Invoice One; Invoice Two; Collection; Registration Stop; Court; Write Off; 
 --       Bankruptcy; Deceased; UTC (Uniform Traffic Citation); Paid;  .  
 -- Sunny will provide extract rule to determine status once the status is finalized between Xerox and FTE. 
 **/

CURSOR C1 IS SELECT 
 -- Valid status: (xerox internal values 0100 to 0200) 
 -- FTE:  Unbilled; Invoice One; Invoice Two; Collection; Registration Stop; Court; Write Off; 
 --       Bankruptcy; Deceased; UTC (Uniform Traffic Citation); Paid;  .  
-- Sunny will provide extract rule to determine status once the status is finalized between Xerox and FTE. 
    NULL EVENT_TYPE     
    ,NULL PREV_EVENT_TYPE 
    ,CASE WHEN va.BANKRUPTCY_FLAG is NOT NULL then 'BANKRUPTCY'
          WHEN va.COLL_COURT_FLAG = 'COLL' and va.DOCUMENT_ID IS NOT NULL and va.CHILD_DOC_ID IS NOT NULL then 'COLLECTION'
          WHEN va.COLL_COURT_FLAG = 'CRT' and va.DOCUMENT_ID IS NOT NULL and va.CHILD_DOC_ID IS NOT NULL then 'COURT'
          WHEN va.COLL_COURT_FLAG is NOT NULL then NULL
          WHEN va.DOCUMENT_ID IS NULL THEN 'UNBILLED'
          WHEN va.CHILD_DOC_ID IS NULL THEN 'INVOICED'
          WHEN va.CHILD_DOC_ID LIKE '%-%' THEN 'UTC'  VIOL_TX_STATUS
          WHEN va.CHILD_DOC_ID IS NOT NULL THEN 'ESCALATED'
          ELSE NULL
     END VIOL_TX_STATUS
    ,NULL PREV_VIOL_TX_STATUS
    ,NULL EVENT_TIMESTAMP -- ** Discussion 

    ,kl.ACCT_NUM ETC_ACCOUNT_ID   -- Join KS_LEDGER on TXN_ID ** add to internal Xerox Discussion
    ,lt.VEH_LIC_NUM PLATE_NUMBER
    ,(select STATE_CODE_ABBR from PA_STATE_CODE where STATE_CODE_NUM=lt.STATE_ID_CODE)
        PLATE_STATE  -- JOIN TO PA_STATE_CODE RETURN STATE_CODE_ABBR
    ,NULL PLATE_COUNTRY -- LOOKUP PA_STATE_CODE. Xerox - internal lookup in DM_ADDRESS_INFO and assign country correctly. visit DM_VEHICLE_INFO and DM_ACCT_INFO
    ,NULL MAKE_ID
    
--IF BANKRUPTCY_FLAG is not null, then 'BANKRUPTCY'
--  JOIN KS_LEDGER.ID = VB_ACTVITY.LEDGER_ID  -- for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
--  JOIN KS_LEDGER.PA_LANE_TXN_ID = PA_LANE_TXN.TXN_ID
--  JOIN PA_PLAZA.PLAZA_ID = PA_Lane_txn.EXT_PLAZA_ID for
--  IF SUM (VB_ACTIVITY.AMT_CHARGED – VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS 
--  grouped by KS_LEDGER.ACCT_NUM
--  FOR PA_PLAZA.FTE_PLAZA = 'Y',
--      VB_ACTIVITY.COLL_COURT_FLAG is NULL, 
--      VB_ACTIVITY.CHILD_DOC_ID not null,
--      VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and 
--      VB_ACTIVITY.BANKRUPTCY_flag is null
--  THEN 'REG STOP'
--    ,CASE WHEN va.BANKRUPTCY_FLAG is NOT null THEN 'BANKRUPTCY'
--          WHEN va.COLL_COURT_FLAG is null and
--               va.DOCUMENT_ID is null THEN 'UNBILLED'
--          WHEN va.COLL_COURT_FLAG is null and
--               va.DOCUMENT_ID is NOT null and
--               va.CHILD_DOC_ID is null THEN 'INVOICED'
--          WHEN va.COLL_COURT_FLAG is null and
--               va.DOCUMENT_ID is NOT null and
--               va.CHILD_DOC_ID is NOT null THEN 'ESCALATED'
--          WHEN va.COLL_COURT_FLAG is null and
--               va.DOCUMENT_ID is NOT null and
--               va.CHILD_DOC_ID like '%-%' THEN 'UTC'
--          WHEN va.COLL_COURT_FLAG is NOT null and
--               va.DOCUMENT_ID is NOT null and
--               va.CHILD_DOC_ID is NOT null and
--               va.COLL_COURT_FLAG = 'COLL' THEN 'COLLECTION'
--          WHEN va.COLL_COURT_FLAG is NOT null and
--               va.DOCUMENT_ID is NOT null and
--               va.CHILD_DOC_ID is NOT null and
--               va.COLL_COURT_FLAG = 'CRT' THEN 'COURT'
--          ELSE NULL
--      END DMV_PLATE_TYPE  -- Derived
    ,NULL DMV_PLATE_TYPE  -- Derived
    
    ,0 REVIEWED_VEHICLE_TYPE
    ,0 REVIEWED_CLASS
    ,3 VIOL_TYPE
    ,0 OUTPUT_FILE_ID
    ,'**' OUTPUT_FILE_TYPE
    ,EXT_DATE_TIME TX_TIMESTAMP   -- time only
    ,trunc(EXT_DATE_TIME) TX_DATE -- date only
    
---- JOIN DOCUMENT_ID OF ST_DOCUMENT_INFO WITH CHILD_DOC_ID
---- JOIN ST_DOCUMENT_INFO.DOCUMENT_ID = CHILD_DOC_ID
--IF STATUS = 'UTC' SET STATUS TO UTC_MAIL  GO TO MAILON ACTIVITY FOR MAILED_ON_DATE
--ELSE 0
    ,CASE WHEN va.COLL_COURT_FLAG is null and
               va.DOCUMENT_ID is NOT null and
               va.CHILD_DOC_ID like '%-%' 
          THEN 1  -- UTC_MAIL
          ELSE 0
      END CITATION_LEVEL  -- Derived  
    
  -- JOIN DOCUMENT_ID OF ST_DOCUMENT_INFO WITH CHILD_DOC_ID
--IF STATUS = 'UTC' SET STATUS TO UTC_MAIL  GO TO MAILON ACTIVITY FOR MAILED_ON_DATE
--    ,DECODE(VIOL_TX_STATUS,'UTC', UTC_MAIL)
    ,CASE WHEN va.COLL_COURT_FLAG is null and
               va.DOCUMENT_ID is NOT null and
               va.CHILD_DOC_ID like '%-%' 
          THEN 1  -- UTC_MAIL
          ELSE 0
      END CITATION_STATUS  -- Derived  

 -- IF VIOL_TX_STATUS = 'UTC' then $25 ELSE 0 
     ,CASE WHEN va.COLL_COURT_FLAG is null and
               va.DOCUMENT_ID is NOT null and
               va.CHILD_DOC_ID like '%-%' 
          THEN 25
          ELSE 0
      END NOTICE_FEE_AMOUNT  -- Derived    

 -- TOTAL_AMT_PAID JOIN WITH KS_LEDGER ON LEDGER_ID AND KS_LEDGER TO PA_LANE_TXN on PA_LANE_TXN_ID
--    ,kl.AMOUNT AMOUNT_PAID
    ,lt.TOLL_AMT_COLLECTED AMOUNT_PAID
    ,lt.TOLL_AMT_CHARGED DISCOUNTED_AMOUNT
    ,0 IMAGE_BATCH_ID
    ,0 IMAGE_BATCH_SEQ_NUMBER
    ,NULL RECON_STATUS_IND  -- XEROX - TO FOLLOW UP
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PATRON.PA_LANE_TXN  lt
    ,PATRON.KS_LEDGER kl
--    ,PATRON.ST_DOCUMENT_INFO di
    ,PATRON.VB_ACTIVITY va
--    ,PATRON.ST_ACTIVITY_PAID ap
WHERE lt.txn_id = kl.PA_LANE_TXN_ID
  AND kl.ID = va.LEDGER_ID (+)
--  AND di.DOCUMENT_ID = va.DOCUMENT_ID (+)  
--  AND di.DOCUMENT_ID = ap.DOCUMENT_ID (+)  
;   -- source

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VIOL_TX_EVENT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */
    /*ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_VIOL_TX_EVENT_INFO_tab.first .. DM_VIOL_TX_EVENT_INFO_tab.last
           INSERT INTO DM_VIOL_TX_EVENT_INFO VALUES DM_VIOL_TX_EVENT_INFO_tab(i);
                       
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


