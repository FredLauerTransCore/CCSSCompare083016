/********************************************************
*
* Name: DM_EMPLOYEE_INFO_PROC
* Created by: DT, 4/21/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_EMPLOYEE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_EMPLOYEE_INFO_PROC IS

TYPE DM_EMPLOYEE_INFO_TYP IS TABLE OF DM_EMPLOYEE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_EMPLOYEE_INFO_tab DM_EMPLOYEE_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    EMP_NAME FIRST_NAME
    ,EMP_NAME LAST_NAME
    ,NULL ACTIVE_FLAG
    ,EMP_CODE EMP_NUM
    ,DECODE(ACCESS_LEVEL, '99','SYSTEM_ADMIN',
                          '95','DEPT_MANAGER',
                          '90','MANAGER','35','CSR_SUPERVISOR',
                          '10','CSR_SUPERVISOR',
                          '01','VIEW_ONLY',
                          '00','NO ACCESS') JOB_TITLE
    ,'00000' MID_NAME
    ,to_date('01/01/1900','MM/DD/YYYY') BIRTH_DT
    ,'00000' STORE_NAME
    ,'SUNPASS' SOURCE_SYSTEM
    ,NULL CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,NULL LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,TEAM EMP_STATUS
FROM PA_EMP_CODE;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_EMPLOYEE_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_EMPLOYEE_INFO_tab.first .. DM_EMPLOYEE_INFO_tab.last
           INSERT INTO DM_EMPLOYEE_INFO VALUES DM_EMPLOYEE_INFO_tab(i);
                       
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


