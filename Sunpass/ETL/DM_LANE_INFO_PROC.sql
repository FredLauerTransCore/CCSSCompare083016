/********************************************************
*
* Name: DM_LANE_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_LANE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_LANE_INFO_PROC IS

TYPE DM_LANE_INFO_TYP IS TABLE OF DM_LANE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_LANE_INFO_tab DM_LANE_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL LANE_ID
    ,NULL EXTERN_LANE_ID
    ,NULL AVI
    ,NULL OPERATIONAL_MODE
    ,NULL STATUS
    ,NULL PLAZA_ID
    ,NULL LANE_IDX
    ,NULL UPDATE_TS
    ,NULL DIRECTION
    ,NULL LANE_TYPE
    ,NULL LANE_MASK
    ,NULL LANE_IP
    ,NULL PORT_NO
    ,NULL HOST_QUEUE_NAME
    ,NULL CALCULATE_TOLL_AMOUNT
    ,NULL IMAGE_RESOLUTION
    ,NULL LPR_ENABLED
    ,NULL ROADWAY
    ,NULL STMT_DESC
    ,NULL LOCATION
    ,NULL COURT_ID
    ,NULL JURISDICTION
    ,NULL LANE_PLAZA_ABBR
    ,NULL AGENCY_ID
    ,NULL SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_LANE_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_LANE_INFO_tab.first .. DM_LANE_INFO_tab.last
           INSERT INTO DM_LANE_INFO VALUES DM_LANE_INFO_tab(i);
                       
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


