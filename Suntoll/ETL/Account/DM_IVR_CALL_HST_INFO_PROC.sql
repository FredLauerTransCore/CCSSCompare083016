/********************************************************
*
* Name: DM_IVR_CALL_HST_INFO_PROC
* Created by: RH, 5/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_IVR_CALL_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

declare
--CREATE OR REPLACE PROCEDURE DM_IVR_CALL_HST_INFO_PROC IS


TYPE DM_IVR_CALL_HST_INFO_TYP IS TABLE OF DM_IVR_CALL_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_IVR_CALL_HST_INFO_tab DM_IVR_CALL_HST_INFO_TYP;

P_ARRAY_SIZE NUMBER:=1000;

-- * Note (terminated employees are inactive)

CURSOR C1 IS SELECT 
    ANI
    ,CALL_ID
    ,DISPOSITION
    ,END_TIME
    ,FAX_NUM
    ,START_TIME
--    ,'SUNTOLL' SOURCE_SYSTEM
FROM PATRON.IVR_CALL
--where rownum<201
;   -- Source table SUNTOLL

SQL_STRING  varchar2(500) := 'truncate table ';
LOAD_TAB    varchar2(50) := 'DM_IVR_CALL_HST_INFO';
ROW_CNT NUMBER := 0;

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  SQL_STRING := SQL_STRING||LOAD_TAB;
  DBMS_OUTPUT.PUT_LINE('SQL_STRING : '||SQL_STRING);
  execute immediate SQL_STRING;
  commit;

  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_IVR_CALL_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_IVR_CALL_HST_INFO_tab.first .. DM_IVR_CALL_HST_INFO_tab.last
           INSERT INTO DM_IVR_CALL_HST_INFO VALUES DM_IVR_CALL_HST_INFO_tab(i);

--    DBMS_OUTPUT.PUT_LINE('Inserted '||sql%rowcount||' into '||LOAD_TAB||' at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
    ROW_CNT := ROW_CNT +  sql%rowcount;                 
    DBMS_OUTPUT.PUT_LINE('ROW count : '||ROW_CNT);
    EXIT WHEN C1%NOTFOUND;
  END LOOP;

  COMMIT;

  CLOSE C1;

  COMMIT;
  DBMS_OUTPUT.PUT_LINE('END '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total ROW count : '||ROW_CNT);
 -- DBMS_OUTPUT.PUT_LINE('END Count '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
END;
/
SHOW ERRORS


