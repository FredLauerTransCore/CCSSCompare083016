/********************************************************
*
* Name: DM_REBILL_HST_INFO_PROC
* Created by: RH, 5/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_REBILL_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_REBILL_HST_INFO_PROC IS

TYPE DM_REBILL_HST_INFO_TYP IS TABLE OF DM_REBILL_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_REBILL_HST_INFO_tab DM_REBILL_HST_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    aw.ACCT_NUM ACCOUNT_NUMBER 
    ,cc.CODE PAY_TYPE
    ,cc.CC_TOKEN CREDIT_CARD_NUMBER_MASK
    ,cc.CC_TOKEN CREDIT_CARD_NUMBER
    ,to_char(to_date(cc.EXPIRATION_DATE,'MM/YY'),'MM') CC_EXP_MONTH
    ,to_char(to_date(cc.EXPIRATION_DATE,'MM/YY'),'YYYY') CC_EXP_YEAR
    ,substr(cc.CC_TOKEN,12,4) LAST_4_CC_NUMBER
    ,NULL ACH_BANK_ACCOUNT_NUMBER -- 'N/A'
    ,NULL ACH_BANK_ACCOUNT_NUMBER_MASK -- 'N/A'
    ,NULL ACH_BANK_ROUTING_NUMBER -- 'N/A'
    ,NULL LAST_4_ACH_NUMBER -- 'N/A'
    ,1 SEQUENCE_NUMBER
    ,cc.CREATED_DATE CREATED
    ,aw.CREATED_BY CREATED_BY 
    ,NULL LAST_UPD -- 'N/A'
    ,NULL LAST_UPD_BY -- 'N/A'
    ,aw.LAST_USED_DATE LAST_USED_DATE  -- PA_ACCT_WALLET
    ,'SUNTOLL' SOURCE_SYSTEM
    ,NULL ACTIVITY_ID
    ,NULL VERSION
    ,NULL CARD_STATUS
    ,NULL EXPDATE
    ,NULL EMP_NUM
    ,NULL X_PRIMARY
    ,NULL LASTREQUEST_REBILL
    ,NULL LASTREQUEST_TRANSID
    ,NULL BOUNCE_STATUS
    ,NULL LETTER_SENT_DATE
    ,NULL DECLINE_STATUS
    ,NULL REBILL_PAY_TYPE_INTEGRATION_ID
    ,NULL STATUS
    ,NULL TYPE
FROM PATRON.PA_ACCT_WALLET aw
    ,PATRON.PA_CREDIT_CARD cc
WHERE aw.CARD_SEQ = cc.CARD_SEQ
    ; -- Source
/*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_REBILL_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_REBILL_HST_INFO_tab.first .. DM_REBILL_HST_INFO_tab.last
           INSERT INTO DM_REBILL_HST_INFO VALUES DM_REBILL_HST_INFO_tab(i);
                       
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

