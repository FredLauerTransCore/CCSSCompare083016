/********************************************************
*
* Name: DM_ACCNT_NOTE_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCNT_NOTE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_ACCNT_NOTE_INFO_PROC IS

TYPE DM_ACCNT_NOTE_INFO_TYP IS TABLE OF DM_ACCNT_NOTE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCNT_NOTE_INFO_tab DM_ACCNT_NOTE_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL ACCOUNT_NUMBER
    ,NULL NOTE_TYPE
    ,NULL NOTE_SUB_TYPE
    ,NULL NOTE
    ,NULL NOTE_CREATED_DT
    ,NULL CREATED
    ,NULL CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,NULL SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCNT_NOTE_INFO_tab
    LIMIT P_ARRAY_SIZE;
    EXIT WHEN C1%NOTFOUND;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ACCNT_NOTE_INFO_tab.first .. DM_ACCNT_NOTE_INFO_tab.last
           INSERT INTO DM_ACCNT_NOTE_INFO VALUES DM_ACCNT_NOTE_INFO_tab(i);
                       

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


