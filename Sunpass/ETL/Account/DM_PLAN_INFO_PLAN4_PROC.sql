/********************************************************
*
* Name: DM_PLAN_INFO_PLAN4_PROC
* Created by: DT, 4/25/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PLAN_INFO_PLAN4
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PLAN_INFO_PLAN4_PROC IS

TYPE DM_PLAN_INFO_PLAN4_TYP IS TABLE OF DM_PLAN_INFO_PLAN4%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PLAN_INFO_PLAN4_tab DM_PLAN_INFO_PLAN4_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,'FHP EMPLOYEES' PLAN_NAME
    ,NULL DEVICE_NUMBER
    ,NULL START_DATE
    ,NULL END_DATE
    ,NULL PLAN_STATUS
    ,NULL AUTO_RENEW
    ,NULL CREATED
    ,'20000' CREATED_BY
    ,NULL LAST_UPD
    ,'20000' LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM FHP_ACCOUNTS;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PLAN_INFO_PLAN4_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    FOR i in DM_PLAN_INFO_PLAN4_tab.first .. DM_PLAN_INFO_PLAN4_tab.last loop

    /* get PA_ACCT.ACCT_OPEN_DATE for START_DATE */
    begin
      select ACCT_OPEN_DATE,
      ACCT_OPEN_DATE,
      ACCT_OPEN_DATE
      into DM_PLAN_INFO_PLAN4_tab(i).START_DATE, 
      DM_PLAN_INFO_PLAN4_tab(i).LAST_UPD,
      DM_PLAN_INFO_PLAN4_tab(i).CREATED
      from PA_ACCT 
      where ACCT_NUM=DM_PLAN_INFO_PLAN4_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_PLAN_INFO_PLAN4_tab(i).START_DATE:=null;
    end;

    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_PLAN_INFO_PLAN4_tab.first .. DM_PLAN_INFO_PLAN4_tab.last
           INSERT INTO DM_PLAN_INFO_PLAN4 VALUES DM_PLAN_INFO_PLAN4_tab(i);
                       
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


