/********************************************************
*
* Name: DM_ADDRESS_HST_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ADDRESS_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_ADDRESS_HST_INFO_PROC IS

TYPE DM_ADDRESS_HST_INFO_TYP IS TABLE OF DM_ADDRESS_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ADDRESS_HST_INFO_tab DM_ADDRESS_HST_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL ACCOUNT_NUMBER
    ,NULL ADDR_TYPE
    ,NULL ADDR_TYPE_INT_ID
    ,NULL STREET_1
    ,NULL STREET_2
    ,NULL CITY
    ,NULL STATE
    ,NULL ZIP_CODE
    ,NULL ZIP_PLUS4
    ,NULL COUNTRY
    ,NULL NIXIE
    ,NULL NIXIE_DATE
    ,NULL NCOA_FLAG
    ,NULL ADDRESS_CLEANSED_FLG
    ,NULL ADDRESS_SOURCE
    ,NULL CREATED
    ,NULL CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,NULL COUNTY_CODE
    ,NULL SOURCE_SYSTEM
    ,NULL ACTIVITY_ID
    ,NULL VERSION
    ,NULL EMP_NUM
    ,NULL STATUS
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ADDRESS_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;
    EXIT WHEN C1%NOTFOUND;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ADDRESS_HST_INFO_tab.first .. DM_ADDRESS_HST_INFO_tab.last
           INSERT INTO DM_ADDRESS_HST_INFO VALUES DM_ADDRESS_HST_INFO_tab(i);
                       

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


