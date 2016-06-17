set ver on
set echo on

create or replace PROCEDURE DM_VEHICLE_INFO_PROC IS

TYPE DM_VEHICLE_INFO_TYP IS TABLE OF DM_VEHICLE_INFO%ROWTYPE
     INDEX BY BINARY_INTEGER;
DM_VEHICLE_INFO_tab DM_VEHICLE_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,DECODE(VEH_LIC_NUM, 'O', '0', VEH_LIC_NUM) PLATE_NUMBER
    ,STATE_STATE_CODE_ABBR PLATE_STATE
    ,NULL PLATE_COUNTRY
    ,VEH_LIC_TYPE PLATE_TYPE
    ,'NA' VEHICLE_TYPE
    ,START_DATE EFFECTIVE_START_DATE
    ,END_DATE EFFECTIVE_END_DATE
    ,decode(translate(lpad(VEH_MODEL_YR,4,'9'),'0123456789','9999999999'),'9999',VEH_MODEL_YR,null) YEAR
    ,VEH_MAKE MAKE
    ,VEH_MODEL MODEL
    ,VEH_COLOR COLOUR
    ,REG_STOP_DATE REG_STOP_START_DATE
    ,END_DATE REG_STOP_END_DATE
    ,VEH_METAL_OX_WS METAL_OXIDE_WIND_SHIELD
    ,to_date('01/01/1900','MM/DD/YYYY') DMV_RETURN_DATE
    ,NULL VEHICLE_CLASS
    ,NULL AXLE_COUNT
    ,NULL IS_DUAL_TIRE
    ,RENTAL_FLAG IS_RENTAL
    ,'NA' RENTAL_COMPANY
    ,'NA' RENTAL_COMPANY_PHONE
    ,CREATION_DATE CREATED
    ,EMP_EMP_CODE CREATED_BY
    ,LAST_MODIFIED_DATE LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,VEHICLE_SEQ_NUM VEHICLE_SEQUENCE
    ,'SUNPASS' SOURCE_SYSTEM
    ,NULL PLATE_ISSUE_DATE
    ,NULL PLATE_EXPIRY_DATE
    ,NULL PLATE_RENEWAL_DATE
    ,NULL ALT_VEHICLE_ID
FROM PA_ACCT_VEHICLE;

BEGIN

  OPEN C1;

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VEHICLE_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    /*ETL SECTION END   */

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
show errors