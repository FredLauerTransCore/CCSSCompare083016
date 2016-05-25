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
    NULL EVENT_TYPE     -- Sunny will provide extract rule to determine status once the status is finalized between Xerox and FTE. 
    ,NULL PREV_EVENT_TYPE 
    ,NULL VIOL_TX_STATUS
    ,NULL PREV_VIOL_TX_STATUS
    ,NULL EVENT_TIMESTAMP -- ** Discussion 
    ,NULL ETC_ACCOUNT_ID   -- Join KS_LEDGER on TXN_ID ** add to internal Xerox Discussion
    ,VEH_LIC_NUM PLATE_NUMBER
    ,STATE_ID_CODE PLATE_STATE  -- JOIN TO PA_STATE_CODE RETURN STATE_CODE_ABBR
    ,NULL PLATE_COUNTRY -- LOOKUP PA_STATE_CODE. Xerox - internal lookup in DM_ADDRESS_INFO and assign country correctly. visit DM_VEHICLE_INFO and DM_ACCT_INFO
    ,NULL MAKE_ID
    
--IF BANKRUPTCY_FLAG is not null, then 'BANKRUPTCY'
--JOIN ID of KS_LEDGER to LEDGER_ID of VB_ACTVITY for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
--JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID of PA_LANE_LANE_TXN and JOIN PLAZA_ID of PA_PLAZA to EXT_PLAZA_ID for PA_Lane_txn
--  IF SUM (VB_ACTIVITY.AMT_CHARGED – VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS grouped by KS_LEDGER.ACCT_NUM
--  FOR PA_PLAZA.FTE_PLAZA = 'Y',VB_ACTIVITY.COLL_COURT_FLAG is NULL, VB_ACTIVITY.CHILD_DOC_ID not null,VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and VB_ACTIVITY.BANKRUPTCY_flag is null
--  THEN 'REG STOP'

    ,NULL DMV_PLATE_TYPE
    ,0 REVIEWED_VEHICLE_TYPE
    ,0 REVIEWED_CLASS
    ,3 VIOL_TYPE
    ,0 OUTPUT_FILE_ID
    ,'**' OUTPUT_FILE_TYPE
    ,EXT_DATE_TIME TX_TIMESTAMP   -- time only
    ,trunc(EXT_DATE_TIME) TX_DATE -- date only
---- JOIN DOCUMENT_ID OF ST_DOCUMENT_INFO WITH CHILD_DOC_ID
--IF STATUS = 'UTC' SET STATUS TO UTC_MAIL  GO TO MAILON ACTIVITY FOR MAILED_ON_DATE
--ELSE 0
    ,NULL CITATION_LEVEL  
  -- JOIN DOCUMENT_ID OF ST_DOCUMENT_INFO WITH CHILD_DOC_ID
--IF STATUS = 'UTC' SET STATUS TO UTC_MAIL  GO TO MAILON ACTIVITY FOR MAILED_ON_DATE
    ,NULL CITATION_STATUS 
    ,DECODE(VIOL_TX_STATUS,'UTC', 25, 0) NOTICE_FEE_AMOUNT -- IF VIOL_TX_STATUS = 'UTC' then $25 ELSE 0
    ,NULL AMOUNT_PAID -- TOTAL_AMT_PAID JOIN WITH KS_LEDGER ON LEDGER_ID AND KS_LEDGER TO PA_LANE_TXN on PA_LANE_TXN_ID
    ,TOLL_AMT_CHARGED DISCOUNTED_AMOUNT
    ,0 IMAGE_BATCH_ID
    ,0 IMAGE_BATCH_SEQ_NUMBER
    ,NULL RECON_STATUS_IND  -- XEROX - TO FOLLOW UP
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_LANE_TXN  lt; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VIOL_TX_EVENT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

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


