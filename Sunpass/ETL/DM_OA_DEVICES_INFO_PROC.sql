/********************************************************
*
* Name: DM_OA_DEVICES_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_OA_DEVICES_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_OA_DEVICES_INFO_PROC IS

TYPE DM_OA_DEVICES_INFO_TYP IS TABLE OF DM_OA_DEVICES_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_OA_DEVICES_INFO_tab DM_OA_DEVICES_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL DEVICE_NO
    ,NULL START_DATE
    ,NULL END_DATE
    ,NULL INVALID_ADDRESS_ID
    ,NULL LAST_FILE_TS
    ,NULL INFILE_IND
    ,NULL UPDATE_TS
    ,NULL FILE_AGENCY_ID
    ,NULL IAG_TAG_STATUS
    ,NULL SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_OA_DEVICES_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_OA_DEVICES_INFO_tab.first .. DM_OA_DEVICES_INFO_tab.last
           INSERT INTO DM_OA_DEVICES_INFO VALUES DM_OA_DEVICES_INFO_tab(i);
                       
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


