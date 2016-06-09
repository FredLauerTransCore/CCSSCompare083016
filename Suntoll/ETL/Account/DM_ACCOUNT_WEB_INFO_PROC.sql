/********************************************************
*
* Name: DM_ACCOUNT_WEB_INFO_PROC
* Created by: RH, 5/19/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCOUNT_WEB_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

declare
--CREATE OR REPLACE PROCEDURE DM_ACCOUNT_WEB_INFO_PROC IS

TYPE DM_ACCOUNT_WEB_INFO_TYP IS TABLE OF DM_ACCOUNT_WEB_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_WEB_INFO_tab DM_ACCOUNT_WEB_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;

-- EXTRACT RULE 
-- Join id of KS_USER to user_id of KS_USER_PA_ACCT_ASSOC to get external user acct_num

CURSOR C1 IS SELECT 
--    ETC_ACCOUNT_ID ETC_ACCOUNT_ID
    ua.PA_ACCT_NUM ETC_ACCOUNT_ID
    ,u.USERNAME USER_NAME
    ,u.PASSWORD PASSWORD  -- (Encrypted) How?
    ,u.LAST_LOGIN_DATE LAST_LOGIN_DATETIME
    ,u.ACCT_LOCKED_UNTIL_DATE INVALID_LOGIN_DATETIME
    ,u.LOGIN_ATTEMPT_COUNT INVALID_LOGIN_COUNT
    ,decode(u.ACCT_LOCKED_UNTIL_DATE,'Y','N') PASSWORD_RESET  --Derived- IF INVALID LOGIN DATETIME IS NOT NULL THEN YES ELSE NO
    ,NULL UPDATE_TS
    ,NULL LAST_LOGOUT_DATETIME
    ,NULL LOGIN_IP_ADDRESS  -- LOGIN_IP ADDRESS of MAX(LOGIN_DATE) from PA_WEB_LOGIN_INFO
    ,NULL USER_AGENT  -- PA_WEB_LOGIN_INFO
    ,ic.ANI ANI  -- IVR_CALL
    ,ic.START_TIME IVR_CALL_START_TIME  -- IVR_CALL
    ,ic.END_TIME IVR_CALL_END_TIME  -- IVR_CALL
    ,NULL LAST_IVR_CALL_DATE  -- SELECT MAX(LAST_LOGIN_DATE) from PA_WEB_LOGIN_INFO WHERE USER AGENT = 'IVR'
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PATRON.KS_USER_PA_ACCT_ASSOC ua
    ,PATRON.KS_USER u
--    ,PA_WEB_LOGIN_INFO wl
    ,IVR_CALL ic
    ,IVR_CALL_DETAIL icd
WHERE ua.USER_ID = u.ID
AND ua.PA_ACCT_NUM = icd.ACCT_NUM -- (+) ?
AND icd.CALL_ID = ic.CALL_ID
--AND ACTIVE_FLAG = 'Y'
--AND   ua.PA_ACCT_NUM = wi.ACCT_NUM (+)
and rownum<1001
; -- Source SunToll

--PA_WEB_LOGIN_INFO
--IVR_CALL -- Using IVR_CALL_DETAIL ? ACCT_NUM ?

SQL_STRING  varchar2(500) := 'truncate table ';
LOAD_TAB    varchar2(50) := 'DM_ACCOUNT_WEB_INFO';
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
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_WEB_INFO_tab
    LIMIT P_ARRAY_SIZE;

-- ETL SECTION BEGIN
    FOR j IN 1 .. DM_ACCOUNT_WEB_INFO_tab.COUNT LOOP
     
     select max(wi.LOGIN_DATE) into  DM_ACCOUNT_WEB_INFO_tab.LAST_IVR_CALL_DATE(j)
      from PA_WEB_LOGIN_INFO wl
      where wl.ACCT_NUM = DM_ACCOUNT_WEB_INFO_tab.ACCOUNT_NUMBER(j)
      and wl.USER_AGENT = 'IVR'
      ;
     select wi.LOGIN_IP, wi.USER_AGENT into  DM_ACCOUNT_WEB_INFO_tab.LOGIN_IP_ADDRESS(j), DM_ACCOUNT_WEB_INFO_tab.USER_AGENT(j)
      from PA_WEB_LOGIN_INFO wl
      where wl.ACCT_NUM = DM_ACCOUNT_WEB_INFO_tab.ACCOUNT_NUMBER(j)
      and wl.LOGIN_DATE = (select max(LOGIN_DATE)
                            from PA_WEB_LOGIN_INFO wl2
                            where wl2.ACCT_NUM = wl.ACCT_NUM)
      ;

    END LOOP;
-- ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_WEB_INFO_tab.first .. DM_ACCOUNT_WEB_INFO_tab.last
           INSERT INTO DM_ACCOUNT_WEB_INFO VALUES DM_ACCOUNT_WEB_INFO_tab(i);
                       
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


