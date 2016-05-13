/********************************************************
*
* Name: DM_BOX_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_BOX_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_BOX_INFO_PROC IS

TYPE DM_BOX_INFO_TYP IS TABLE OF DM_BOX_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_BOX_INFO_tab DM_BOX_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL BOX_NUMBER
    ,NULL BOX_TYPE
    ,NULL BOX_STATUS
    ,NULL DEVICE_MODEL
    ,NULL VEHICLE_CLASS
    ,NULL STORE
    ,NULL DEVICE_COUNT
    ,NULL START_DEVICE_NUMBER
    ,NULL END_DEVICE_NUMBER
    ,NULL BOX_SHELF_RACK#
    ,NULL BOX_CHECKOUT_TO
    ,NULL BOX_CHECKOUT_BY
    ,NULL DISPOSE_DATE
    ,NULL CREATED
    ,NULL CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,NULL SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_BOX_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_BOX_INFO_tab.first .. DM_BOX_INFO_tab.last
           INSERT INTO DM_BOX_INFO VALUES DM_BOX_INFO_tab(i);
                       
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


