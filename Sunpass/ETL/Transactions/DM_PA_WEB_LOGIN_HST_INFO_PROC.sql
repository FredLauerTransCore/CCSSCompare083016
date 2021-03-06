/********************************************************
*
* Name: DM_PA_WEB_LOGIN_HST_INF_PROC
* Created by: DT, 5/3/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PA_WEB_LOGIN_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PA_WEB_LOGIN_HST_INF_PROC IS

TYPE DM_PA_WEB_LOGIN_HST_INF_TYP IS TABLE OF DM_PA_WEB_LOGIN_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PA_WEB_LOGIN_HST_INF_tab DM_PA_WEB_LOGIN_HST_INF_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    PA_WEB_LOGIN_INFO.ACCT_NUM ACCT_NUM
    ,PA_WEB_LOGIN_INFO.LOGIN_DATE LOGIN_DATE
    ,PA_WEB_LOGIN_INFO.LOGIN_IP LOGIN_IP
    ,PA_WEB_LOGIN_INFO.USER_AGENT USER_AGENT
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_WEB_LOGIN_INFO;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PA_WEB_LOGIN_HST_INF_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_PA_WEB_LOGIN_HST_INF_tab.count loop
	 if DM_PA_WEB_LOGIN_HST_INF_tab(i).ACCT_NUM is null then
          DM_PA_WEB_LOGIN_HST_INF_tab(i).ACCT_NUM:='0';
         end if;
	 if DM_PA_WEB_LOGIN_HST_INF_tab(i).LOGIN_DATE is null then
          DM_PA_WEB_LOGIN_HST_INF_tab(i).LOGIN_DATE:=sysdate;
         end if;
	 if DM_PA_WEB_LOGIN_HST_INF_tab(i).LOGIN_IP is null then
          DM_PA_WEB_LOGIN_HST_INF_tab(i).LOGIN_IP:='0';
         end if;
	 if DM_PA_WEB_LOGIN_HST_INF_tab(i).USER_AGENT is null then
          DM_PA_WEB_LOGIN_HST_INF_tab(i).USER_AGENT:='0';
         end if;
    end loop;

	
    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_PA_WEB_LOGIN_HST_INF_tab.first .. DM_PA_WEB_LOGIN_HST_INF_tab.last
           INSERT INTO DM_PA_WEB_LOGIN_HST_INFO VALUES DM_PA_WEB_LOGIN_HST_INF_tab(i);
                       
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


