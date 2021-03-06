/********************************************************
*
* Name: DM_PLAN_INFO_PLAN3_PROC
* Created by: DT, 4/24/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PLAN_INFO_PLAN3
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PLAN_INFO_PLAN3_PROC IS

TYPE DM_PLAN_INFO_PLAN3_TYP IS TABLE OF DM_PLAN_INFO_PLAN3%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PLAN_INFO_PLAN3_tab DM_PLAN_INFO_PLAN3_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,'NON REVENUE' PLAN_NAME
    ,INVTRANSP_TRANSP_TRANSP_ID DEVICE_NUMBER
    ,SALE_DATE START_DATE
    ,NULL END_DATE
    ,REVCLASS_REV_CLASS_CODE PLAN_STATUS
    ,'N' AUTO_RENEW
    ,SALE_DATE CREATED
    ,decode(EMP_EMP_CODE,NULL,'20000', EMP_EMP_CODE) CREATED_BY
    ,SALE_DATE LAST_UPD
    ,decode(EMP_EMP_CODE,NULL,'20000', EMP_EMP_CODE) LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_ACCT_TRANSP
WHERE REVCLASS_REV_CLASS_CODE in ('03','04');

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PLAN_INFO_PLAN3_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_PLAN_INFO_PLAN3_tab.count loop
	 if DM_PLAN_INFO_PLAN3_tab(i).ACCOUNT_NUMBER is null then
          DM_PLAN_INFO_PLAN3_tab(i).ACCOUNT_NUMBER:='0';
         end if;
	 if DM_PLAN_INFO_PLAN3_tab(i).PLAN_NAME is null then
          DM_PLAN_INFO_PLAN3_tab(i).PLAN_NAME:='0';
         end if;
	 if DM_PLAN_INFO_PLAN3_tab(i).PLAN_STATUS is null then
          DM_PLAN_INFO_PLAN3_tab(i).PLAN_STATUS:='0';
         end if;
	 if DM_PLAN_INFO_PLAN3_tab(i).CREATED is null then
          DM_PLAN_INFO_PLAN3_tab(i).CREATED:=sysdate;
         end if;
	 if DM_PLAN_INFO_PLAN3_tab(i).LAST_UPD is null then
          DM_PLAN_INFO_PLAN3_tab(i).LAST_UPD:=sysdate;
         end if;
    end loop;

	
    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_PLAN_INFO_PLAN3_tab.first .. DM_PLAN_INFO_PLAN3_tab.last
           INSERT INTO DM_PLAN_INFO_PLAN3 VALUES DM_PLAN_INFO_PLAN3_tab(i);
                       
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


