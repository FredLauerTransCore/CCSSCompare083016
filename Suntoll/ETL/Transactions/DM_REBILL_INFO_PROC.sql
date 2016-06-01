/********************************************************
*
* Name: DM_REBILL_INFO_PROC
* Created by: RH, 5/18/2016
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
    aw.ACCT_NUM ACCOUNT_NUMBER 
    ,cc.CODE PAY_TYPE
    ,cc.CC_TOKEN CREDIT_CARD_NUMBER_MASK
    ,cc.CC_TOKEN CREDIT_CARD_NUMBER
    ,to_char(to_date(cc.EXPIRATION_DATE,'MM/YY'),'MM') CC_EXP_MONTH
    ,to_char(to_date(cc.EXPIRATION_DATE,'MM/YY'),'YYYY') CC_EXP_YEAR
    ,substr(cc.CC_TOKEN,12,4) LAST_4_CC_NUMBER
    ,NULL ACH_BANK_ACCOUNT_NUMBER
    ,NULL ACH_BANK_ACCOUNT_NUMBER_MASK
    ,NULL ACH_BANK_ROUTING_NUMBER
    ,NULL LAST_4_ACH_NUMBER
    ,'1' SEQUENCE_NUMBER -- DEFAULT to 1 ; FTE - We need the value possibilities, and what the sequence numbers are
    ,cc.CREATED_DATE CREATED
    ,aw.CREATED_BY CREATED_BY 
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,aw.LAST_USED_DATE LAST_USED_DATE  -- PA_ACCT_WALLET
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PATRON.PA_ACCT_WALLET aw
    ,PATRON.PA_CREDIT_CARD cc
WHERE aw.CARD_SEQ = cc.CARD_SEQ
    ; -- Source

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_REBILL_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

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


