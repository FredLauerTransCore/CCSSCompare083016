/********************************************************
*
* Name: DM_RETAILER_INFO_PROC
* Created by: DT, 4/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_RETAILER_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_RETAILER_INFO_PROC IS

TYPE DM_RETAILER_INFO_TYP IS TABLE OF DM_RETAILER_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_RETAILER_INFO_tab DM_RETAILER_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,L_NAME||' ' || F_NAME ACCOUNT_NAME
    ,'FTE' AGENCY
    ,ACCTTYPE_ACCT_TYPE_CODE ACCOUNT_TYPE
    ,ACCTSTAT_ACCT_STATUS_CODE ACCOUNT_STATUS
    ,NULL ACCOUNT_OPEN_MODE
    ,ACCT_OPEN_DATE RETAILER_ACCOUNT_OPEN_DT
    ,NULL PIN
    ,NULL DBA_NAME
    ,0 COMM_PER_DEVICE
    ,NULL CLOSURE_REASON
    ,ACCT_CLOSE_DATE CLOSE_DATE
    ,NULL COMENTS
    ,E_MAIL_ADDR EMAIL_ADDRESS
    ,substr(ORG,1,30) STORE_NAME
    ,NULL LOGIN_NAME
    ,NULL COMPANY_URL
    ,NULL CHALLENGE_QUESTION
    ,NULL CHALLENGE_ANSWER
    ,ACCT_OPEN_DATE CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,to_date('02/27/2017','MM/DD/YYYY') LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_ACCT;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_RETAILER_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */
	
	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_RETAILER_INFO_tab.count loop
	 if DM_RETAILER_INFO_tab(i).ACCOUNT_NUMBER is null then
          DM_RETAILER_INFO_tab(i).ACCOUNT_NUMBER:='0';
         end if;
	 if DM_RETAILER_INFO_tab(i).ACCOUNT_NAME is null then
          DM_RETAILER_INFO_tab(i).ACCOUNT_NAME:='0';
         end if;
	 if DM_RETAILER_INFO_tab(i).ACCOUNT_STATUS is null then
          DM_RETAILER_INFO_tab(i).ACCOUNT_STATUS:='0';
         end if;
	 if DM_RETAILER_INFO_tab(i).RETAILER_ACCOUNT_OPEN_DT is null then
          DM_RETAILER_INFO_tab(i).RETAILER_ACCOUNT_OPEN_DT:=sysdate;
         end if;
	 if DM_RETAILER_INFO_tab(i).COMM_PER_DEVICE is null then
          DM_RETAILER_INFO_tab(i).COMM_PER_DEVICE:='0';
         end if;
	 if DM_RETAILER_INFO_tab(i).EMAIL_ADDRESS is null then
          DM_RETAILER_INFO_tab(i).EMAIL_ADDRESS:='0';
         end if;
	 if DM_RETAILER_INFO_tab(i).LOGIN_NAME is null then
          DM_RETAILER_INFO_tab(i).LOGIN_NAME:='0';
         end if;
	 if DM_RETAILER_INFO_tab(i).CREATED is null then
          DM_RETAILER_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_RETAILER_INFO_tab(i).LAST_UPD is null then
          DM_RETAILER_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_RETAILER_INFO_tab.first .. DM_RETAILER_INFO_tab.last
           INSERT INTO DM_RETAILER_INFO VALUES DM_RETAILER_INFO_tab(i);
                       
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


