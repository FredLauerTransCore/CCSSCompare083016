/********************************************************
*
* Name: DM_REBILL_INFO_PROC
* Created by: DT, 4/26/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_REBILL_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_REBILL_INFO_PROC IS

TYPE DM_REBILL_INFO_TYP IS TABLE OF DM_REBILL_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_REBILL_INFO_tab DM_REBILL_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,CREDITCARD_CREDIT_CARD_CODE PAY_TYPE
    ,rpad('*',length(REPL_NUM)-5,'*')||substr(REPL_NUM,-4) CREDIT_CARD_NUMBER_MASK
    ,REPL_NUM CREDIT_CARD_NUMBER
    ,substr(REPL_DATE,1,2) CC_EXP_MONTH
    ,substr(REPL_DATE,3,4) CC_EXP_YEAR
    ,substr(REPL_NUM,-4) LAST_4_CC_NUMBER
    ,NULL ACH_BANK_ACCOUNT_NUMBER
    ,NULL ACH_BANK_ACCOUNT_NUMBER_MASK
    ,NULL ACH_BANK_ROUTING_NUMBER
    ,NULL LAST_4_ACH_NUMBER
    ,NULL SEQUENCE_NUMBER
    ,NULL CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,LAST_UPDATE_DATE LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,NULL LAST_USED_DATE
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_ACCT_REPL_DETAIL;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_REBILL_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */
	
	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_REBILL_INFO_tab.count loop
	 if DM_REBILL_INFO_tab(i).ACCOUNT_NUMBER is null then
          DM_REBILL_INFO_tab(i).ACCOUNT_NUMBER:='0';
         end if;
	 if DM_REBILL_INFO_tab(i).PAY_TYPE is null then
          DM_REBILL_INFO_tab(i).PAY_TYPE:='0';
         end if;
	 if DM_REBILL_INFO_tab(i).CREATED is null then
          DM_REBILL_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_REBILL_INFO_tab(i).LAST_UPD is null then
          DM_REBILL_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
	 if DM_REBILL_INFO_tab(i).LAST_USED_DATE is null then
          DM_REBILL_INFO_tab(i).LAST_USED_DATE:=sysdate;
         end if;
    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_REBILL_INFO_tab.first .. DM_REBILL_INFO_tab.last
           INSERT INTO DM_REBILL_INFO VALUES DM_REBILL_INFO_tab(i);
                       
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


