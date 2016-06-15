/********************************************************
*
* Name: DM_ACCNT_NOTE_INFO_PROC
* Created by: RH, 5/17/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCNT_NOTE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

--DECLARE
CREATE OR REPLACE PROCEDURE DM_ACCNT_NOTE_INFO_PROC IS

TYPE DM_ACCNT_NOTE_INFO_TYP IS TABLE OF DM_ACCNT_NOTE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCNT_NOTE_INFO_tab DM_ACCNT_NOTE_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,PROB_CODE NOTE_TYPE
    ,TYPE_CODE NOTE_SUB_TYPE
    ,NOTE NOTE
    ,CREATED NOTE_CREATED_DT
    ,CREATED CREATED
    ,EMP_CODE CREATED_BY
    ,CREATED LAST_UPD
    ,EMP_CODE LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
FROM NOTE
where rownum<11
;   -- Source

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCNT_NOTE_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ACCNT_NOTE_INFO_tab.first .. DM_ACCNT_NOTE_INFO_tab.last
           INSERT INTO DM_ACCNT_NOTE_INFO VALUES DM_ACCNT_NOTE_INFO_tab(i);
                       
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

