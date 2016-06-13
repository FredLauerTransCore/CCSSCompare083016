/********************************************************
*
* Name: DM_BOX_INFO_PROC
* Created by: DT, 4/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_BOX_INFO

We need to get more info:
1. how to get the STORE from PA_ACCT.ACCTTYPE_ACCT_TYPE_CODE
2. how to get BOX_CHECKOUT_TO and BOX_CHECKOUT_BY
3. How to get CREATED and CREATED_BY from PA_ACCT_TRANSP
4. How to get LAST_UPD, LAST_UPD_BY

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
    TRAY_TRAY_NUM BOX_NUMBER 
    ,TRAY_CASE_CASE_NUM BOX_TYPE 
    ,NULL BOX_STATUS 
    ,TRANSPTYPE_TRANSP_TYPE_CODE DEVICE_MODEL 
    ,VEHCLASS_VEH_CLASS_CODE VEHICLE_CLASS 
    ,ACCTTYPE_ACCT_TYPE_CODE STORE 
    ,NULL DEVICE_COUNT 
    ,NULL START_DEVICE_NUMBER 
    ,NULL END_DEVICE_NUMBER 
    ,NULL BOX_SHELF_RACK# 
    ,DECODE(EMP_EMP_CODE,
            'SUNPASS', EMP_CODE,
            'SUNTOLL', EMP_CODE + 10000,
            'DEFAULT', 20000) BOX_CHECKOUT_TO  --EMP_EMP_CODE and EMP_CODE Not in Select
            
    ,DECODE(EMP_EMP_CODE,
            'SUNPASS', EMP_CODE,
            'SUNTOLL', EMP_CODE + 10000,
            'DEFAULT', 20000) BOX_CHECKOUT_BY  --EMP_EMP_CODE and EMP_CODE Not in Select       
    ,NULL BOX_CHECKOUT_BY 
    ,NULL DISPOSE_DATE 
    ,CREATED CREATED 
    ,NULL CREATED_BY 
    ,MAX(ISSUE_DATE) LAST_UPD --Verify if applicable per comment in
    ,NULL LAST_UPD_BY 
    ,'SUNPASS' SOURCE_SYSTEM 
FROM PATRON.PA_INV_TRANSP;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_BOX_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in DM_DEVICE_INFO_tab.first .. DM_DEVICE_INFO_tab.last loop

    /* get PA_INV_TRANSP_ASSGN.EMP_EMP_CODE for BOX_CHECKOUT_TO */
     begin
        select EMP_EMP_CODE into DM_ACCOUNT_INFO_tab(i).BOX_CHECKOUT_TO from PA_INV_TRANSP_ASSGN where TRAY_TRAY_NUM=PA_INV_TRANSP_tab(i).TRAY_TRAY_NUM;
        exception when others then null;
       DM_ACCOUNT_INFO_tab(i).BOX_CHECKOUT_TO:=null;
     end;


    end loop;

    /*ETL SECTION END   */


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


