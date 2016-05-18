/********************************************************
*
* Name: DM_EMPLOYEE_INFO_PROC
* Created by: RH, 4/13/2016
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

-- * Note (terminated employees are inactive)

CURSOR C1 IS SELECT 
    F_NAME FIRST_NAME
    ,L_NAME LAST_NAME
    ,substr(STATUS,1,1) ACTIVE_FLAG   --Indicates whether Employee is Active (A) or Inactive (I)
    ,LEGACY_EMP_CODE EMP_NUM
    ,USER_TYPE_CODE JOB_TITLE
    ,M_INITIAL MID_NAME
    ,NULL BIRTH_DT
    ,LOCATION_ID STORE_NAME
    ,'SUNTOLL' SOURCE_SYSTEM
    ,CREATED_ON CREATED
    ,CREATED_BY_USER_ID CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
FROM PATRON.KS_USER;   -- Source table SUNTOLL


BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_EMPLOYEE_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

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


