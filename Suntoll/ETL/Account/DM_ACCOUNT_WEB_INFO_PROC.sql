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

CREATE OR REPLACE PROCEDURE DM_ACCOUNT_WEB_INFO_PROC IS

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
    ,NULL USER_AGENT
--    ,ic.ANI ANI
--    ,ic.START_TIME IVR_CALL_START_TIME
--    ,ic.END_TIME IVR_CALL_END_TIME
    ,NULL LAST_IVR_CALL_DATE  -- SELECT MAX(LAST_LOGIN_DATE) from PA_WEB_LOGIN_INFO WHERE USER AGENT = 'IVR'
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PATRON.KS_USER_PA_ACCT_ASSOC ua
    ,PATRON.KS_USER u
--    ,PA_WEB_LOGIN_INFO wl
--    ,IVR_CALL ic
WHERE ua.USER_ID = u.ID
--AND   ua.
--AND   ua.PA_ACCT_NUM = wi.ACCT_NUM (+)
; -- Source SunToll

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_WEB_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_WEB_INFO_tab.first .. DM_ACCOUNT_WEB_INFO_tab.last
           INSERT INTO DM_ACCOUNT_WEB_INFO VALUES DM_ACCOUNT_WEB_INFO_tab(i);
                       
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


