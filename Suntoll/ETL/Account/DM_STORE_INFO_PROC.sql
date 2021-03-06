/********************************************************
*
* Name: DM_STORE_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_STORE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_STORE_INFO_PROC IS

TYPE DM_STORE_INFO_TYP IS TABLE OF DM_STORE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_STORE_INFO_tab DM_STORE_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL AGENCY
    ,NULL CONTACT_PERSON_NAME
    ,NULL CARRY_INVENTORY
    ,NULL ADDR_LINE1
    ,NULL ADDR_LINE2
    ,NULL CITY
    ,'FL' STATE
    ,'USA' COUNTRY
    ,NULL PHONE_NUMBER
    ,NULL FAX_NUMBER
    ,NULL RETAILER_FLAG
    ,PLAZA_NAME STORE_NAME
    ,NULL STORE_STATUS
    ,NULL STORE_START_DATE
    ,NULL STORE_END_DATE
    ,NULL DATE_CLOSED
    ,NULL DEFAULT_LOGON_MODE
    ,NULL ZIP_CODE
    ,NULL ZIP_PLUS
    ,NULL IS_WALKIN_STORE
    ,PLAZA_ID STORE_ID
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_PLAZA; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_STORE_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_STORE_INFO_tab.first .. DM_STORE_INFO_tab.last
           INSERT INTO DM_STORE_INFO VALUES DM_STORE_INFO_tab(i);              

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


