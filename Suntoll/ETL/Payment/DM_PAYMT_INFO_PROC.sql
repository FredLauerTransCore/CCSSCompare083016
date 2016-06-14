/********************************************************
*
* Name: DM_PAYMT_INFO_PROC
* Created by: RH, 5/17/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PAYMT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

DECLARE

--CREATE OR REPLACE PROCEDURE DM_PAYMT_INFO_PROC IS

TYPE DM_PAYMT_INFO_TYP IS TABLE OF DM_PAYMT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PAYMT_INFO_tab DM_PAYMT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=1000;

-- 6/14/16 JL Updated REVERSED and NSF_FEE mapping to NULL based on 6/10 discussion with FTE and Xerox.

CURSOR C1 IS SELECT 
    pp.ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,pp.PUR_TRANS_DATE TX_DT  -- PA_PURCHASE
    ,trim(nvl(pay.PAYTYPE_PAYMENT_TYPE_CODE,'None') PAY_TYPE
    ,trim(nvl(pd.PRODUCT_PUR_PRODUCT_CODE,'R') TRAN_TYPE
--    ,(select PROD_ABBRV from PA_PUR_PRODUCT where PUR_PRODUCT_CODE = pd.PRODUCT_PUR_PRODUCT_CODE) TRAN_TYPE    ?
    ,nvl(pay.PUR_PAY_AMT,0) AMOUNT
--    ,NULL REVERSED  -- PA_PURCHASE_DETAIL.PRODUCT_PUR_PRODUCT_CODE 
    ,NULL REVERSED  -- PA_PURCHASE_DETAIL.PRODUCT_PUR_PRODUCT_CODE 
----    ,(select PROD_ABBRV from PA_PUR_PRODUCT where PUR_PRODUCT_CODE = pd.PRODUCT_PUR_PRODUCT_CODE) REVERSED    ?
    ,to_char(to_date(pay.PUR_CREDIT_EXP_DATE,'MM/YY'),'MM') EXP_MONTH
    ,to_char(to_date(pay.PUR_CREDIT_EXP_DATE,'MM/YY'),'YYYY') EXP_YEAR
    ,trim(pay.PUR_CREDIT_ACCT_NUM) CREDIT_CARD_NUMBER
    ,trim(pay.CHK_ACCT_NUM) BANK_ACCOUNT_NUMBER
    ,trim(pay.ABA_NUMBER) BANK_ROUTING_NUMBER
    ,trim(pay.CHECK_NUM) CHECK_NUMBER
-- DERIVED  PA_PURCHASE_DETAIL.RODUCT_PUR_PRODUCT_CODE ?
    ,NULL NSF_FEE -- DERIVED  PA_PURCHASE_DETAIL.RODUCT_PUR_PRODUCT_CODE ?
    ,pay.PUR_PUR_ID PAYMENT_REFERENCE_NUM   -- unique
    ,trim(pay.EMP_EMP_CODE) EMPLOYEE_NUMBER    
    ,nvl(pay.PUR_PAY_ID,0) TRANSACTION_ID
-- Derived from product code and VES_REF_NUM from PA_PURCHASE_DETAIL table. Default Derived
    ,nvl(pd.VES_REF_NUM,'Derived') ORG_TRANSACTION_ID 
--    ,(select PROD_ABBRV from PA_PUR_PRODUCT where PUR_PRODUCT_CODE = pd.PRODUCT_PUR_PRODUCT_CODE) REVERSED    
    ,NULL XREF_TRANSACTION_ID  -- No mapping
    ,pay.PUR_CREDIT_AUTH_NUM CC_RESPONSE_CODE
    ,pay.CC_GATEWAY_REF_ID EXTERNAL_REFERENCE_NUM
    ,NULL MISS_APPLIED
    ,NULL DESCRIPTION  -- No mapping
-- If REF_APPROVED_DATE is populated, then the refund status will be populated. REF_DECLINE_REASON_ID will get the status of the refund.
-- 1=UNAPPROVED, 2=APPROVED, 3=PARTIAL, 4=REJECTED.
    ,CASE WHEN rr.REF_APPROVED_DATE IS NOT NULL THEN 
        decode(rr.REFUND_STATUS_ID, 1,'UNAPPROVED', 2,'APPROVED', 3,'PARTIAL', 4,'REJECTED',NULL) 
      END REFUND_STATUS 
    ,NULL REFUND_CHECK_NUM    ,'N/A' TOD_ID -- N/A
    ,pp.PUR_TRANS_DATE CREATED -- PA_PURCHASE
    ,pay.EMP_EMP_CODE CREATED_BY
    ,trunc(SYSDATE) LAST_UPD  -- M/A
    ,NULL LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
    ,pay.CC_GATEWAY_REQ_ID CC_GATEWAY_REQ_ID
    ,pay.CC_TXN_REF_NUM CC_TXN_REF_NUM
FROM PA_PURCHASE pp
    ,PA_PURCHASE_PAYMENT pay
    ,PA_PURCHASE_DETAIL pd
    ,PA_REFUND_REQUEST rr
WHERE pp.PUR_ID = pay.PUR_PUR_ID
AND   pp.PUR_ID = pd.PUR_PUR_ID (+)
AND   pp.PUR_ID = rr.PUR_PUR_ID (+); -- source

SQL_STRING  varchar2(500) := 'truncate table ';
LOAD_TAB    varchar2(50) := 'DM_PAYMT_INFO';
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
    FETCH C1 BULK COLLECT INTO DM_PAYMT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_PAYMT_INFO_tab.first .. DM_PAYMT_INFO_tab.last
           INSERT INTO DM_PAYMT_INFO VALUES DM_PAYMT_INFO_tab(i);
           
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


