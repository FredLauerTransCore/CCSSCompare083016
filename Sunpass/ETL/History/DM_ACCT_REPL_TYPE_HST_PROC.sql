/********************************************************
*
* Name: DM_ACCT_REPL_TYPE_HST_PROC
* Created by: DT, 5/8/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCT_REPL_TYPE_HST
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_ACCT_REPL_TYPE_HST_PROC IS

TYPE DM_ACCT_REPL_TYPE_HST_TYP IS TABLE OF DM_ACCT_REPL_TYPE_HST%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCT_REPL_TYPE_HST_tab DM_ACCT_REPL_TYPE_HST_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCT_ACCT_NUM
    ,FROM_REPL_TYPE_CODE FROM_REPL_TYPE_CODE
    ,TO_REPL_TYPE_CODE TO_REPL_TYPE_CODE
    ,MODIFIED_DATE_TIME MODIFIED_DATE_TIME
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_ACCT_REPL_TYPE_HISTORY;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCT_REPL_TYPE_HST_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_ACCT_REPL_TYPE_HST_tab.first .. DM_ACCT_REPL_TYPE_HST_tab.last
           INSERT INTO DM_ACCT_REPL_TYPE_HST VALUES DM_ACCT_REPL_TYPE_HST_tab(i);
                       
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


