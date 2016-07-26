/********************************************************
*
* Name: DM_VEHICLE_HST_INFO_PROC
* Created by: DT, 5/3/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_VEHICLE_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_VEHICLE_HST_INFO_PROC IS

TYPE DM_VEHICLE_HST_INFO_TYP IS TABLE OF DM_VEHICLE_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_VEHICLE_HST_INFO_tab DM_VEHICLE_HST_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,DECODE(VEH_LIC_NUM, 'O', '0', VEH_LIC_NUM) PLATE_NUMBER
    ,STATE_STATE_CODE_ABBR PLATE_STATE
    ,NULL PLATE_COUNTRY
    ,VEH_LIC_TYPE||STATE_STATE_CODE_ABBR PLATE_TYPE  
    ,'NA' VEHICLE_TYPE
    ,START_DATE EFFECTIVE_START_DATE
    ,END_DATE EFFECTIVE_END_DATE
    ,'9999' YEAR
    ,VEH_MAKE MAKE
    ,VEH_MODEL MODEL
    ,VEH_COLOR COLOUR
    ,REG_STOP_DATE REG_STOP_START_DATE
    ,NULL REG_STOP_END_DATE
    ,VEH_METAL_OX_WS METAL_OXIDE_WIND_SHIELD
    ,to_date('01/01/1900','MM/DD/YYYY') DMV_RETURN_DATE
    ,null VEHICLE_CLASS
    ,null AXLE_COUNT
    ,null IS_DUAL_TIRE
    ,RENTAL_FLAG IS_RENTAL
    ,null RENTAL_COMPANY
    ,null RENTAL_COMPANY_PHONE
    ,CREATION_DATE CREATED
    ,EMP_EMP_CODE CREATED_BY
    ,LAST_MODIFIED_DATE LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,VEHICLE_SEQ_NUM VEHICLE_SEQUENCE
    ,'SUNPASS' SOURCE_SYSTEM
    ,'0' ACTIVITY_ID
    ,NULL REBILL_PAY_TYPE_INTEGRATION_ID
    ,NULL EMP_NUM
    ,'0' IS_MVA
    ,NULL X_LICENSE_STATUS
    ,'0' X_PLATE_VIN_NO
    ,'0' STATUS
    ,NULL X_ALTERNATEID
    ,NULL X_OWNER
    ,NULL X_WEB_VEHICLE_TYPE
	,NULL VERSION
FROM PA_ACCT_VEHICLE;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VEHICLE_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */
	
    FOR i in 1 .. DM_VEHICLE_HST_INFO_tab.count loop

      begin
        select COUNTRY into DM_VEHICLE_HST_INFO_tab(i).PLATE_COUNTRY from COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_VEHICLE_HST_INFO_tab(i).PLATE_STATE
        and rownum<=1;
      exception
        when others then null;
        DM_VEHICLE_HST_INFO_tab(i).PLATE_COUNTRY:='USA';
      end;
      DM_VEHICLE_HST_INFO_tab(i).PLATE_TYPE := DM_VEHICLE_HST_INFO_tab(i).PLATE_COUNTRY||DM_VEHICLE_HST_INFO_tab(i).PLATE_TYPE;
      
	    /* to default the values NOT NULL columns */
	 if DM_VEHICLE_HST_INFO_tab(i).ACCOUNT_NUMBER is null then
          DM_VEHICLE_HST_INFO_tab(i).ACCOUNT_NUMBER:='0';
         end if;
	 if DM_VEHICLE_HST_INFO_tab(i).PLATE_NUMBER is null then
          DM_VEHICLE_HST_INFO_tab(i).PLATE_NUMBER:='0';
         end if;
	 if DM_VEHICLE_HST_INFO_tab(i).PLATE_STATE is null then
          DM_VEHICLE_HST_INFO_tab(i).PLATE_STATE:='FL';
         end if;
	 if DM_VEHICLE_HST_INFO_tab(i).PLATE_TYPE is null then
          DM_VEHICLE_HST_INFO_tab(i).PLATE_TYPE:='0';
         end if;
	 if DM_VEHICLE_HST_INFO_tab(i).MAKE is null then
          DM_VEHICLE_HST_INFO_tab(i).MAKE:='0';
         end if;
	 if DM_VEHICLE_HST_INFO_tab(i).VEHICLE_CLASS is null then
          DM_VEHICLE_HST_INFO_tab(i).VEHICLE_CLASS:='0';
         end if;
	 if DM_VEHICLE_HST_INFO_tab(i).CREATED is null then
          DM_VEHICLE_HST_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_VEHICLE_HST_INFO_tab(i).LAST_UPD is null then
          DM_VEHICLE_HST_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
	 if DM_VEHICLE_HST_INFO_tab(i).VEHICLE_SEQUENCE is null then
          DM_VEHICLE_HST_INFO_tab(i).VEHICLE_SEQUENCE:='0';
         end if;
    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_VEHICLE_HST_INFO_tab.first .. DM_VEHICLE_HST_INFO_tab.last
           INSERT INTO DM_VEHICLE_HST_INFO VALUES DM_VEHICLE_HST_INFO_tab(i);
                       
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


