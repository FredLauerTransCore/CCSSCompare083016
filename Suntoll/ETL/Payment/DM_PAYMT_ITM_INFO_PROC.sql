/********************************************************
*
* Name: DM_PAYMT_ITM_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PAYMT_ITM_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

declare
--CREATE OR REPLACE PROCEDURE DM_PAYMT_ITM_INFO_PROC IS

TYPE DM_PAYMT_ITM_INFO_TYP IS TABLE OF DM_PAYMT_ITM_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PAYMT_ITM_INFO_tab DM_PAYMT_ITM_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    pd.PRODUCT_PUR_PRODUCT_CODE CATEGORY
    ,pd.PRODUCT_PUR_PRODUCT_CODE SUB_CATEGORY
    ,pd.PRODUCT_PUR_PRODUCT_CODE TRANS_TYPE
--    ,(select PROD_ABBRV from PA_PUR_PRODUCT where PUR_PRODUCT_CODE = pd.PRODUCT_PUR_PRODUCT_CODE) TRANS_TYPE    
    ,nvl(pd.PROD_AMT,0) AMOUNT
    ,'FTE' ENDO_AGENCY
    ,'FTE' REVENUE_AGENCY
    ,(select pur.PUR_PRODUCT_DESC from PA_PUR_PRODUCT pur
        where pur.PUR_PRODUCT_CODE = pd.PRODUCT_PUR_PRODUCT_CODE) DESCRIPTION   
    ,NULL TRANSFER_REASON
    ,NULL NOTICE_NUMBER
    ,NULL SEQUENCE_NUMBER
    ,pp.PUR_ID PARENT_PAYMENT_ID   -- PA_PURCHASE
    ,pp.PUR_TRANS_DATE CREATED   -- PA_PURCHASE
--    ,pay.EMP_EMP_CODE CREATED_BY  -- OR below
    ,(select pay.EMP_EMP_CODE from PA_PURCHASE_PAYMENT pay
        where pay.PUR_PUR_ID = pp.PUR_ID) CREATED_BY   
    ,SYSDATE LAST_UPD  -- N/A
    ,NULL LAST_UPD_BY -- N/A
    ,pp.PUR_ID INVOICE_NUM   -- PA_PURCHASE
    ,NULL INVOICE_REF_ID
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_PURCHASE pp
    ,PA_PURCHASE_DETAIL pd
--    ,PA_PURCHASE_PAYMENT pay
WHERE pp.PUR_ID = pd.PUR_PUR_ID  -- (+)
--AND   pp.PUR_ID = pay.PUR_PUR_ID (+)
and rownum<10001
; -- source

SQL_STRING  varchar2(500) := 'truncate table ';
LOAD_TAB    varchar2(50)  := 'DM_PAYMT_ITM_INFO';
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
    FETCH C1 BULK COLLECT INTO DM_PAYMT_ITM_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_PAYMT_ITM_INFO_tab.first .. DM_PAYMT_ITM_INFO_tab.last
           INSERT INTO DM_PAYMT_ITM_INFO VALUES DM_PAYMT_ITM_INFO_tab(i);
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


