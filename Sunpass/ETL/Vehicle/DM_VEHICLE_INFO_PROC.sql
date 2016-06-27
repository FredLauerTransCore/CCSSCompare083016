/********************************************************
*
* Name: DM_VEHICLE_INFO_PROC
* Created by: RH, 4/13/2016
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
    ,(SELECT COUNTRY FROM COUNTRY_STATE_LOOKUP WHERE ST_ABBR = PA_ACCT.STATE) PLATE_COUNTRY
    --,'USA' PLATE_COUNTRY      -- Derive from State	PLATE_COUNTRY	USA (Default), CAN, MEX
    ,VEH_LIC_TYPE PLATE_TYPE  
    ,'REGULAR' VEHICLE_TYPE   --	"REGULAR (Default), AUTO / SUV ?
    ,START_DATE EFFECTIVE_START_DATE  --MAX(RESPONSE_DATE)Event lookup ROV
    ,END_DATE EFFECTIVE_END_DATE
    ,nvl(VEH_MODEL_YR, '9999') YEAR
    ,nvl(VEH_MAKE, 'OTHER') MAKE
    ,nvl(VEH_MODEL, 'OTHER') MODEL
    ,VEH_COLOR COLOUR
    ,REG_STOP_START_DATE REG_STOP_START_DATE
    ,END_DATE REG_STOP_END_DATE
    ,VEH_METAL_OX_WS METAL_OXIDE_WIND_SHIELD
    ,NULL DMV_RETURN_DATE   -- suntoll only
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
    ,'SUNPASS_CSC_ID' LAST_UPD_BY   --	DEFAULT	SUNPASS_CSC_ID		LAST_UPD_BY	SUNPASS_CSC_ID (Default)
    ,VEHICLE_SEQ_NUM VEHICLE_SEQUENCE
    ,'SUNPASS' SOURCE_SYSTEM
    ,NULL PLATE_ISSUE_DATE
    ,NULL PLATE_EXPIRY_DATE
    ,NULL PLATE_RENEWAL_DATE
    ,NULL ALT_VEHICLE_ID
--  ,VEH_LIC_NUM SRC_LIC_VEH_NUM  -- Missing in Target DM table
FROM PA_ACCT_VEHICLE; 

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VEHICLE_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN */
	
	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_VEHICLE_INFO_tab.count loop
	 if DM_VEHICLE_INFO_tab(i).ACCOUNT_NUMBER is null then
          DM_VEHICLE_INFO_tab(i).ACCOUNT_NUMBER:='0';
         end if;
	 if DM_VEHICLE_INFO_tab(i).PLATE_NUMBER is null then
          DM_VEHICLE_INFO_tab(i).PLATE_NUMBER:='0';
         end if;
	 if DM_VEHICLE_INFO_tab(i).PLATE_STATE is null then
          DM_VEHICLE_INFO_tab(i).PLATE_STATE:='0';
         end if;
	 if DM_VEHICLE_INFO_tab(i).PLATE_COUNTRY is null then
          DM_VEHICLE_INFO_tab(i).PLATE_COUNTRY:='0';
         end if;
	 if DM_VEHICLE_INFO_tab(i).PLATE_TYPE is null then
          DM_VEHICLE_INFO_tab(i).PLATE_TYPE:='0';
         end if;
	 if DM_VEHICLE_INFO_tab(i).YEAR is null then
          DM_VEHICLE_INFO_tab(i).YEAR:='0';
         end if;
	 if DM_VEHICLE_INFO_tab(i).MAKE is null then
          DM_VEHICLE_INFO_tab(i).MAKE:='0';
         end if;
	 if DM_VEHICLE_INFO_tab(i).VEHICLE_CLASS is null then
          DM_VEHICLE_INFO_tab(i).VEHICLE_CLASS:='0';
         end if;
	 if DM_VEHICLE_INFO_tab(i).CREATED is null then
          DM_VEHICLE_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_VEHICLE_INFO_tab(i).LAST_UPD is null then
          DM_VEHICLE_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
	 if DM_VEHICLE_INFO_tab(i).VEHICLE_SEQUENCE is null then
          DM_VEHICLE_INFO_tab(i).VEHICLE_SEQUENCE:='0';
         end if;
    end loop;


     /* ETL SECTION END*/

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


