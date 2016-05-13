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
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,DECODE(VEH_LIC_NUM, 'O', '0', VEH_LIC_NUM) PLATE_NUMBER
    ,STATE_STATE_CODE_ABBR PLATE_STATE
    ,'USA' PLATE_COUNTRY   -- Derive from State	PLATE_COUNTRY	USA (Default), CAN, MEX
    ,VEH_LIC_TYPE PLATE_TYPE    -- Prefix and Suffix for the Country and State.  
                                -- Discussion on how FL prefix & suffixes will be handled is an outstanding action item.
                                -- Sunny will provide sample FL plates for analysis that will have associated type info.
    ,'REGULAR' VEHICLE_TYPE    --	"REGULAR (Default), AUTO / SUV ?
    ,SUBSCRIPTION_START_DATE EFFECTIVE_START_DATE
    ,SUBSCRIPTION_END_DATE EFFECTIVE_END_DATE
    ,nvl(VEH_MODEL_YR, '9999') YEAR
    ,nvl(VEH_MAKE, 'OTHER') MAKE
    ,nvl(VEH_MODEL, 'OTHER') MODEL
    ,VEH_COLOR COLOUR
    ,REG_STOP_SENT_ON REG_STOP_START_DATE  -- ST_REG_STOP
    ,REG_STOP_REMOVAL_SENT_ON REG_STOP_END_DATE -- ST_REG_STOP
    ,NULL METAL_OXIDE_WIND_SHIELD
    ,NULL DMV_RETURN_DATE  -- suntoll    --MAX(RESPONSE_DATE)Event lookup ROV
    ,to_date('01-01-1900', 'MM-DD-YYYY') DMV_RETURN_DATE
    ,NULL VEHICLE_CLASS
    ,NULL AXLE_COUNT
    ,NULL IS_DUAL_TIRE
    ,RENTAL_FLAG IS_RENTAL
    ,NULL RENTAL_COMPANY
    ,NULL RENTAL_COMPANY_PHONE
    ,CREATION_DATE CREATED
    ,EMP_EMP_CODE CREATED_BY
    ,LAST_MODIFIED_DATE LAST_UPD
    ,NULL LAST_UPD_BY   --	DEFAULT	SUNPASS_CSC_ID		LAST_UPD_BY	SUNPASS_CSC_ID (Default)
    ,VEHICLE_SEQ VEHICLE_SEQUENCE
    ,SUNTOLL SOURCE_SYSTEM
    ,NULL PLATE_ISSUE_DATE    -- DERIVE
    ,NULL PLATE_EXPIRY_DATE   -- DERIVE
    ,NULL PLATE_RENEWAL_DATE  -- DERIVE
    ,NULL ALT_VEHICLE_ID      -- DERIVE
FROM PA_ACCT_VEHICLE; 


BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VEHICLE_INFO_tab
    LIMIT P_ARRAY_SIZE;
    EXIT WHEN C1%NOTFOUND;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_VEHICLE_INFO_tab.first .. DM_VEHICLE_INFO_tab.last
           INSERT INTO DM_VEHICLE_INFO VALUES DM_VEHICLE_INFO_tab(i);
                       

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


