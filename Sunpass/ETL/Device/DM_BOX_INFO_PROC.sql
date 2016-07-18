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
    it.TRAY_TRAY_NUM BOX_NUMBER 
    ,it.TRAY_CASE_CASE_NUM BOX_TYPE 
    ,NULL BOX_STATUS 
    ,it.TRANSPTYPE_TRANSP_TYPE_CODE DEVICE_MODEL 
    ,at.VEHCLASS_VEH_CLASS_CODE VEHICLE_CLASS 
    ,'CSC STORE' STORE 
    ,NULL DEVICE_COUNT 
    ,NULL START_DEVICE_NUMBER 
    ,NULL END_DEVICE_NUMBER 
    ,NULL BOX_SHELF_RACK# 
    ,NULL BOX_CHECKOUT_TO
    ,NULL BOX_CHECKOUT_BY
    ,NULL DISPOSE_DATE 
    ,pa.ACCT_OPEN_DATE CREATED 
    ,'SUNPASS_CSC_ID' CREATED_BY 
    ,MAX(it.ISSUE_DATE) LAST_UPD --Verify if applicable per comment in
    ,NULL LAST_UPD_BY 
    ,'SUNPASS' SOURCE_SYSTEM 
FROM PA_INV_TRANSP it
    ,PA_ACCT_TRANSP at
    ,PA_ACCT pa
WHERE at.INVTRANSP_TRANSP_TRANSP_ID = it.INVTRANSP_TRANSP_ID
  AND pa.ACCT_NUM = at.ACCT_ACCT_NUM;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_BOX_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in DM_BOX_INFO_tab.first .. DM_BOX_INFO_tab.last loop

    /* get PA_INV_TRANSP_ASSGN.EMP_EMP_CODE for BOX_CHECKOUT_TO */
     begin
        select EMP_EMP_CODE into DM_BOX_INFO_tab(i).BOX_CHECKOUT_TO from PA_INV_TRANSP_ASSGN where TRAY_TRAY_NUM=DM_BOX_INFO_tab(i).BOX_NUMBER;
        DM_BOX_INFO_tab(i).BOX_CHECKOUT_BY:=DM_BOX_INFO_tab(i).BOX_CHECKOUT_TO;
        exception when others then null;
       DM_BOX_INFO_tab(i).BOX_CHECKOUT_TO:=null;
       DM_BOX_INFO_tab(i).BOX_CHECKOUT_BY:=null;
     end;


    end loop;
	
	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_BOX_INFO_tab.count loop
	 if DM_BOX_INFO_tab(i).BOX_NUMBER is null then
          DM_BOX_INFO_tab(i).BOX_NUMBER:='0';
         end if;
	 if DM_BOX_INFO_tab(i).BOX_TYPE is null then
          DM_BOX_INFO_tab(i).BOX_TYPE:='0';
         end if;
	 if DM_BOX_INFO_tab(i).BOX_STATUS is null then
          DM_BOX_INFO_tab(i).BOX_STATUS:='0';
         end if;
	 if DM_BOX_INFO_tab(i).DEVICE_MODEL is null then
          DM_BOX_INFO_tab(i).DEVICE_MODEL:='0';
         end if;
	 if DM_BOX_INFO_tab(i).STORE is null then
          DM_BOX_INFO_tab(i).STORE:='0';
         end if;
	 if DM_BOX_INFO_tab(i).CREATED is null then
          DM_BOX_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_BOX_INFO_tab(i).LAST_UPD is null then
          DM_BOX_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
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


