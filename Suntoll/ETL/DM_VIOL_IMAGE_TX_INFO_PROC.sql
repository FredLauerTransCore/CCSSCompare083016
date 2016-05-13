/********************************************************
*
* Name: DM_VIOL_IMAGE_TX_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_VIOL_IMAGE_TX_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_VIOL_IMAGE_TX_INFO_PROC IS

TYPE DM_VIOL_IMAGE_TX_INFO_TYP IS TABLE OF DM_VIOL_IMAGE_TX_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_VIOL_IMAGE_TX_INFO_tab DM_VIOL_IMAGE_TX_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL LANE_ID
    ,NULL TX_TIMESTAMP
    ,NULL TX_SEQ_NUMBER
    ,NULL IN_FILE_NAME
    ,NULL OUT_FILE_NAME
    ,NULL PROCESS_DESC
    ,NULL RECEIVE_DATE
    ,NULL PROCESS_DATE
    ,NULL OCR_CONF
    ,NULL OCR_PLATE_STATE
    ,NULL OCR_PLATE_NUMBER
    ,NULL VEH_SEQ_NUMBER
    ,NULL LANE_IMAGE_CNT
    ,NULL PROC_IMAGE_CNT
    ,NULL FTP_TIMESTAMP
    ,NULL UPDATE_TS
    ,NULL OCR_STATUS
    ,NULL TX_DATE
    ,NULL LANE_TX_ID
    ,NULL ZIP_FILE_NAME
    ,NULL TR_FILE_ID
    ,NULL RET_FILE_ID
    ,NULL IMAGE_STATUS
    ,NULL AXLE_COUNT
    ,NULL IMAGE_TAKEN_REASON
    ,NULL PROCESS_FLAG
    ,NULL IMP_TYPE
    ,NULL PLAZA_ID
    ,NULL MATCH_TYPE
    ,NULL FP_CONF
    ,NULL COLOR_BW
    ,NULL OCR_PLATE_COUNTRY
    ,NULL DMV_PLATE_TYPE
    ,NULL IMAGE_TX_ID
    ,NULL REDUCED_PLATE
    ,NULL MANUAL_PLATE_NUMBER
    ,NULL MANUAL_PLATE_STATE
    ,NULL MANUAL_PLATE_TYPE
    ,NULL MANUAL_PLATE_COUNTRY
    ,NULL IMAGE_RVW_CLERK_ID
    ,NULL REVIEWED_VEHICLE_TYPE
    ,NULL IMAGE_REVIEW_TIMESTAMP
    ,NULL REVIEWED_DATE
    ,NULL IMAGE_BATCH_ID
    ,NULL IMAGE_BATCH_SEQ_NUMBER
    ,NULL DEVICE_NO
    ,NULL ACTUAL_AXLES
    ,NULL ACTUAL_CLASS
    ,NULL READ_AVI_CLASS
    ,NULL REVIEWED_CLASS
    ,NULL POSTCLASS_AXLES
    ,NULL FP_STATUS
    ,NULL OCR_THRESHOLD
    ,NULL FP_THRESHOLD
    ,NULL OCR_RUN_MODE
    ,NULL OCR_START_TIMESTAMP
    ,NULL OCR_END_TIMESTAMP
    ,NULL IMG_FILE_INDEX
    ,NULL SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VIOL_IMAGE_TX_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_VIOL_IMAGE_TX_INFO_tab.first .. DM_VIOL_IMAGE_TX_INFO_tab.last
           INSERT INTO DM_VIOL_IMAGE_TX_INFO VALUES DM_VIOL_IMAGE_TX_INFO_tab(i);
                       
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


