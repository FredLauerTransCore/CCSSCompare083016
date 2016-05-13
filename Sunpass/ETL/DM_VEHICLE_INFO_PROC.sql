/********************************************************
*
* Name: DM_VEHICLE_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_VEHICLE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_VEHICLE_INFO_PROC IS

TYPE DM_VEHICLE_INFO_TYP IS TABLE OF DM_VEHICLE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_VEHICLE_INFO_tab DM_VEHICLE_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL ACCOUNT_NUMBER
    ,NULL PLATE_NUMBER
    ,NULL PLATE_STATE
    ,NULL PLATE_COUNTRY
    ,NULL PLATE_TYPE
    ,NULL VEHICLE_TYPE
    ,NULL EFFECTIVE_START_DATE
    ,NULL EFFECTIVE_END_DATE
    ,NULL YEAR
    ,NULL MAKE
    ,NULL MODEL
    ,NULL COLOUR
    ,NULL REG_STOP_START_DATE
    ,NULL REG_STOP_END_DATE
    ,NULL METAL_OXIDE_WIND_SHIELD
    ,NULL DMV_RETURN_DATE
    ,NULL VEHICLE_CLASS
    ,NULL AXLE_COUNT
    ,NULL IS_DUAL_TIRE
    ,NULL IS_RENTAL
    ,NULL RENTAL_COMPANY
    ,NULL RENTAL_COMPANY_PHONE
    ,NULL CREATED
    ,NULL CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,NULL VEHICLE_SEQUENCE
    ,NULL SOURCE_SYSTEM
    ,NULL PLATE_ISSUE_DATE
    ,NULL PLATE_EXPIRY_DATE
    ,NULL PLATE_RENEWAL_DATE
    ,NULL ALT_VEHICLE_ID
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VEHICLE_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_VEHICLE_INFO_tab.first .. DM_VEHICLE_INFO_tab.last
           INSERT INTO DM_VEHICLE_INFO VALUES DM_VEHICLE_INFO_tab(i);
                       
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


