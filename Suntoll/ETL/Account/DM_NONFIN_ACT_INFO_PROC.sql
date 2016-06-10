/********************************************************
*
* Name: DM_NONFIN_ACT_INFO_PROC
* Created by: RH, 6/4/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_NONFIN_ACT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- EMP_CODE IS LIKE '99xx' or '0100' -- SYSTEM

declare
--CREATE OR REPLACE PROCEDURE DM_NONFIN_ACT_INFO_PROC IS

TYPE DM_NONFIN_ACT_INFO_TYP IS TABLE OF DM_NONFIN_ACT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_NONFIN_ACT_INFO_tab DM_NONFIN_ACT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,nvl((SELECT MAX(ACTIVITY_NUMBER + 1) 
        FROM DM_NONFIN_ACT_INFO 
        WHERE ACCT_NUM = NOTE.ACCT_NUM),0) as ACTIVITY_NUMBER
--    ,NULL ACTIVITY_NUMBER
    ,TYPE_CODE CATEGORY
    ,PROB_CODE SUB_CATEGORY
    ,TYPE_ID ACTIVITY_TYPE
    ,NOTE DESCRIPTION
    ,CREATED CREATED
    ,EMP_CODE CREATED_BY
    ,CREATED LAST_UPD
    ,EMP_CODE LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
FROM NOTE
where EMP_CODE like '99%' or EMP_CODE = '0100'
and rownum<101
; 
--

SQL_STRING  varchar2(500) := 'truncate table ';
LOAD_TAB    varchar2(50) := 'DM_NONFIN_ACT_INFO';
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
    FETCH C1 BULK COLLECT INTO DM_NONFIN_ACT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_NONFIN_ACT_INFO_tab.first .. DM_NONFIN_ACT_INFO_tab.last
           INSERT INTO DM_NONFIN_ACT_INFO VALUES DM_NONFIN_ACT_INFO_tab(i);
                       
--    DBMS_OUTPUT.PUT_LINE('Inserted '||sql%rowcount||' into '||LOAD_TAB||' at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
    ROW_CNT := ROW_CNT +  sql%rowcount;                 
    DBMS_OUTPUT.PUT_LINE('Inserted ROW count : '||ROW_CNT);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;

  COMMIT;

  CLOSE C1;

  COMMIT;
  DBMS_OUTPUT.PUT_LINE('End '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total Rows : '||ROW_CNT);

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
END;
/
SHOW ERRORS


