/********************************************************
*
* Name: DM_NONFIN_ACT_INFO_PROC
* Created by: RH, 4/17/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_NONFIN_ACT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_NONFIN_ACT_INFO_PROC IS

TYPE DM_NONFIN_ACT_INFO_TYP IS TABLE OF DM_NONFIN_ACT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_NONFIN_ACT_INFO_tab DM_NONFIN_ACT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,NULL ACTIVITY_NUMBER
    ,NULL CATEGORY
    ,NULL SUB_CATEGORY
    ,NULL ACTIVITY_TYPE
    ,NULL DESCRIPTION
    ,NULL CREATED
    ,NULL CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_NONFIN_ACT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_NONFIN_ACT_INFO_tab.first .. DM_NONFIN_ACT_INFO_tab.last
           INSERT INTO DM_NONFIN_ACT_INFO VALUES DM_NONFIN_ACT_INFO_tab(i);
                       
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

