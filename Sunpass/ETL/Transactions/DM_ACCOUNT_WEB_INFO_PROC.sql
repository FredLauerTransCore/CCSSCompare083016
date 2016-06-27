/********************************************************
*
* Name: DM_ACCOUNT_WEB_INFO_PROC
* Created by: DT, 5/2/2016
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


CURSOR C1 IS SELECT 
    ACCT_NUM ETC_ACCOUNT_ID
    ,ACCT_NUM USER_NAME
    ,ACCT_PIN_NUMBER PASSWORD
    ,NULL LAST_LOGIN_DATETIME
    ,NULL INVALID_LOGIN_DATETIME
    ,NULL INVALID_LOGIN_COUNT
    ,NULL PASSWORD_RESET
    ,NULL UPDATE_TS
    ,NULL LAST_LOGOUT_DATETIME
    ,NULL LOGIN_IP_ADDRESS
    ,NULL USER_AGENT
    , NULL ANI
    , NULL IVR_CALL_START_TIME
    , NULL IVR_CALL_END_TIME
    ,NULL LAST_IVR_CALL_DATE
    ,'SUNPASS' SOURCE_SYSTEM
FROM SUNPASS.PA_ACCT;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_WEB_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */




    FOR i in 1 .. DM_ACCOUNT_WEB_INFO_tab.count loop

    /* get PA_ACCT_SUNPASS.LOGIN_ATTEMPT_COUNT for INVALID_LOGIN_COUNT */
    begin
      select LOGIN_ATTEMPT_COUNT into DM_ACCOUNT_WEB_INFO_tab(i).INVALID_LOGIN_COUNT from PA_ACCT_SUNPASS 
      where ACCT_NUM=DM_ACCOUNT_WEB_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_WEB_INFO_tab(i).INVALID_LOGIN_COUNT:=null;
    end;

    /* get PA_ACCT_SUNPASS.ACCT_LOCKED_TIMESTAMP for INVALID_LOGIN_DATETIME */
    begin
      select ACCT_LOCKED_TIMESTAMP into DM_ACCOUNT_WEB_INFO_tab(i).INVALID_LOGIN_DATETIME from PA_ACCT_SUNPASS 
      where ACCT_NUM=DM_ACCOUNT_WEB_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_WEB_INFO_tab(i).INVALID_LOGIN_DATETIME:=null;
    end;

    /* get PA_ACCT_SUNPASS.decode(sum(decode(ACCT_LOCKED_TIMESTAMP,null,0,1)),1,'Y','N') for PASSWORD_RESET */
    begin
      select decode(sum(decode(ACCT_LOCKED_TIMESTAMP,null,0,1)),1,'Y','N') into DM_ACCOUNT_WEB_INFO_tab(i).PASSWORD_RESET from PA_ACCT_SUNPASS 
      where ACCT_NUM=DM_ACCOUNT_WEB_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_WEB_INFO_tab(i).PASSWORD_RESET:=null;
    end;

    /* get PA_WEB_LOGIN_INFO.LOGIN_DATE for LAST_LOGIN_DATETIME */
    begin
      select LOGIN_DATE into DM_ACCOUNT_WEB_INFO_tab(i).LAST_LOGIN_DATETIME from PA_WEB_LOGIN_INFO 
      where ACCT_NUM=DM_ACCOUNT_WEB_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_WEB_INFO_tab(i).LAST_LOGIN_DATETIME:=null;
    end;

    /* get PA_WEB_LOGIN_INFO.USER_AGENT for USER_AGENT */
    begin
      select USER_AGENT into DM_ACCOUNT_WEB_INFO_tab(i).USER_AGENT from PA_WEB_LOGIN_INFO 
      where ACCT_NUM=DM_ACCOUNT_WEB_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_WEB_INFO_tab(i).USER_AGENT:=null;
    end;

    /* get PA_WEB_LOGIN_INFO.LOGIN_IP ADDRESS of MAX(LOGIN_DATE) from PA_WEB_LOGIN_INFO for GIN_IP_ADDRESS */
    begin
      select LOGIN_IP into DM_ACCOUNT_WEB_INFO_tab(i).LOGIN_IP_ADDRESS from PA_WEB_LOGIN_INFO 
      where ACCT_NUM=DM_ACCOUNT_WEB_INFO_tab(i).ETC_ACCOUNT_ID and 
            LOGIN_DATE=(select MAX(LOGIN_DATE) from PA_WEB_LOGIN_INFO where ACCT_NUM=DM_ACCOUNT_WEB_INFO_tab(i).ETC_ACCOUNT_ID )
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_WEB_INFO_tab(i).LOGIN_IP_ADDRESS:=null;
    end;

    end loop;

 
     /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_ACCOUNT_WEB_INFO_tab.count loop
	 if DM_ACCOUNT_WEB_INFO_tab(i).ETC_ACCOUNT_ID is null then
          DM_ACCOUNT_WEB_INFO_tab(i).ETC_ACCOUNT_ID:='0';
         end if;
	 if DM_ACCOUNT_WEB_INFO_tab(i).USER_NAME is null then
          DM_ACCOUNT_WEB_INFO_tab(i).USER_NAME:='0';
         end if;
	 if DM_ACCOUNT_WEB_INFO_tab(i).PASSWORD is null then
          DM_ACCOUNT_WEB_INFO_tab(i).PASSWORD:='0';
         end if;
	 if DM_ACCOUNT_WEB_INFO_tab(i).LAST_LOGIN_DATETIME is null then
          DM_ACCOUNT_WEB_INFO_tab(i).LAST_LOGIN_DATETIME:=sysdate;
         end if;
	 if DM_ACCOUNT_WEB_INFO_tab(i).INVALID_LOGIN_DATETIME is null then
          DM_ACCOUNT_WEB_INFO_tab(i).INVALID_LOGIN_DATETIME:=sysdate;
         end if;
	 if DM_ACCOUNT_WEB_INFO_tab(i).INVALID_LOGIN_COUNT is null then
          DM_ACCOUNT_WEB_INFO_tab(i).INVALID_LOGIN_COUNT:='0';
         end if;
	 if DM_ACCOUNT_WEB_INFO_tab(i).PASSWORD_RESET is null then
          DM_ACCOUNT_WEB_INFO_tab(i).PASSWORD_RESET:='0';
         end if;
    end loop;



    /*ETL SECTION END   */

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


