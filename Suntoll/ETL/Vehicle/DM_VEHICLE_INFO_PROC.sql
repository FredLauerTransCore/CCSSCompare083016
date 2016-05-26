/********************************************************
*
* Name: DM_VEHICLE_INFO_PROC
* Created by: RH, 4/18/2016
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
    av.ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,DECODE(av.VEH_LIC_NUM, 'O', '0', av.VEH_LIC_NUM) PLATE_NUMBER  -- PATRON.EVENT_LOOKUP_ROV
    ,av.STATE_STATE_CODE_ABBR PLATE_STATE  -- Derive from State	PLATE_COUNTRY	USA (Default), CAN, MEX
    ,'USA' PLATE_COUNTRY   -- Derive from State	PLATE_COUNTRY	USA (Default), CAN, MEX
    ,av.VEH_LIC_TYPE PLATE_TYPE    -- Prefix and Suffix for the Country and State.  
    ,'REGULAR' VEHICLE_TYPE     
    ,SUBSCRIPTION_START_DATE EFFECTIVE_START_DATE
    ,SUBSCRIPTION_END_DATE EFFECTIVE_END_DATE
    ,nvl(av.VEH_MODEL_YR, '9999') YEAR
    ,nvl(av.VEH_MAKE, 'OTHER') MAKE
    ,nvl(av.VEH_MODEL, 'OTHER') MODEL
    ,av.VEH_COLOR COLOUR
    ,rs.REG_STOP_SENT_ON REG_STOP_START_DATE  -- ST_REG_STOP
    ,rs.REG_STOP_REMOVAL_SENT_ON REG_STOP_END_DATE -- ST_REG_STOP
    ,NULL METAL_OXIDE_WIND_SHIELD
    ,NULL DMV_RETURN_DATE  -- suntoll    --MAX(RESPONSE_DATE)Event lookup ROV
    ,NULL VEHICLE_CLASS
    ,NULL AXLE_COUNT
    ,NULL IS_DUAL_TIRE
    ,av.RENTAL_CAR_FLAG IS_RENTAL
    ,NULL RENTAL_COMPANY
    ,NULL RENTAL_COMPANY_PHONE
    ,av.SUBSCRIPTION_START_DATE CREATED
    ,av.EMP_EMP_CODE CREATED_BY
    ,av.LAST_MODIFIED_DATE LAST_UPD
    ,NULL LAST_UPD_BY   --	DEFAULT	SUNPASS_CSC_ID		LAST_UPD_BY	SUNPASS_CSC_ID (Default)
--    ,VEHICLE_SEQ VEHICLE_SEQUENCE
    ,'SUNTOLL' SOURCE_SYSTEM
    ,ev.PLATE_ISSUE_DATE PLATE_ISSUE_DATE    -- DERIVE  PLATE_ISSUE_DATE
    ,ev.REG_EXP_DATE PLATE_EXPIRY_DATE   -- DERIVE  REG_EXP_DATE
    ,ev.DECAL_ISSUE_DATE PLATE_RENEWAL_DATE  -- DERIVE  DECAL_ISSUE_DATE
    ,ev.VIN ALT_VEHICLE_ID      -- DERIVE  VIN
FROM PATRON.PA_ACCT_VEHICLE av
    ,PATRON.ST_REG_STOP rs
    ,PATRON.EVENT_VEHICLE ev
WHERE av.VEH_LIC_NUM = rs.VEH_LIC_NUM (+)
AND av.VEH_LIC_NUM = ev.PLATE (+); 


BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VEHICLE_INFO_tab
    LIMIT P_ARRAY_SIZE;
    EXIT WHEN C1%NOTFOUND;
    -- PATRON.EVENT_LOOKUP_ROV

--ETL SECTION BEGIN mapping
    FOR j IN 1 .. DM_ACCOUNT_INFO_tab.COUNT LOOP
     
      select max(elr.RESPONSE_DATE) into  DM_ACCOUNT_INFO_tab.DMV_RETURN_DATE(j)
      from PATRON.EVENT_LOOKUP_ROV elr
      where elr.PLATE = DM_ACCOUNT_INFO_tab.PLATE_NUMBER(j)
      ;

    END LOOP;
--      ETL SECTION END

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


