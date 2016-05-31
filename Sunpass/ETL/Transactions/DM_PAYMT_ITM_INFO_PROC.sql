/********************************************************
*
* Name: DM_PAYMT_ITM_INFO_PROC
* Created by: DT, 4/20/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PAYMT_ITM_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PAYMT_ITM_INFO_PROC IS

TYPE DM_PAYMT_ITM_INFO_TYP IS TABLE OF DM_PAYMT_ITM_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PAYMT_ITM_INFO_tab DM_PAYMT_ITM_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL CATEGORY
    ,NULL SUB_CATEGORY
    ,NULL TRANS_TYPE
    ,NULL AMOUNT
    ,'FTE' ENDO_AGENCY
    ,'FTE' REVENUE_AGENCY
    ,NULL DESCRIPTION
    ,NULL TRANSFER_REASON
    ,NULL NOTICE_NUMBER
    ,NULL SEQUENCE_NUMBER
    ,PUR_ID PARENT_PAYMENT_ID
    ,PUR_TRANS_DATE CREATED
    ,NULL CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,PUR_ID INVOICE_NUM
    ,NULL INVOICE_REF_ID 
    ,'SUNPASS' SOURCE_SYSTEM
FROM PATRON.PA_PURCHASE;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PAYMT_ITM_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in PA_PURCHASE_tab.first .. DM_PAYMT_ITM_INFO_tab.last loop

    /* get PA_PURCHASE_PAYMENT.EMP_EMP_CODE for CREATED_BY */
    begin
      select EMP_EMP_CODE into PA_PURCHASE_tab(i).CREATED_BY from PA_PURCHASE_PAYMENT       where PUR_PAY_ID=PA_PURCHASE_tab(i).PUR_ID and rownum<=1;
      exception when others then null;
      PA_PURCHASE_tab(i).CREATED_BY:=null;
    end;

    end loop;

    /* get PA_PURCHASE_DETAIL.PRODUCT_PUR_PRODUCT_CODE for T_PRODUCT_PUR_PRODUCT_CODE */
    begin
      select PRODUCT_PUR_PRODUCT_CODE into T_PRODUCT_PUR_PRODUCT_CODE from       PA_PURCHASE_DETAIL 
      where PUR_PUR_ID=PA_PURCHASE_tab(i).PUR_ID
            and rownum<=1;
      exception 
        when others then null;
    end;

    /* get PA_PUR_PRODUCT.PUR_PRODUCT_DESC for DESCRIPTION */
    begin
      select PUR_PRODUCT_DESC into PA_PURCHASE_tab(i).DESCRIPTION from PA_PUR_PRODUCT 
      where PUR_PRODUCT_CODE=T_PRODUCT_PUR_PRODUCT_CODE
            and rownum<=1;
      exception 
        when others then null;
        PA_PURCHASE_tab(i).DESCRIPTION:=null;
    end;

    /* get PA_PURCHASE_DETAIL.PROD_AMT for AMOUNT */
    begin
      select PROD_AMT into PA_PURCHASE_tab(i).AMOUNT from PA_PURCHASE_DETAIL 
      where PUR_PUR_ID=PA_PURCHASE_tab(i).PUR_ID
            and rownum<=1;
      exception 
        when others then null;
        PA_PURCHASE_tab(i).AMOUNT:=null;
    end;



    FOR i in PA_PURCHASE_tab.first .. DM_PAYMT_ITM_INFO_tab.last loop

    end loop;

    /* get PA_PURCHASE_DETAIL.PRODUCT_PUR_PRODUCT_CODE for TRANS_TYPE */
    begin
      select PRODUCT_PUR_PRODUCT_CODE into PA_PURCHASE_tab(i).TRANS_TYPE from       PA_PURCHASE_DETAIL 
      where PUR_PUR_ID=PA_PURCHASE_tab(i).PUR_ID
            and rownum<=1;
      exception 
        when others then null;
        PA_PURCHASE_tab(i).TRANS_TYPE:=null;
    end;

    /* get PA_PURCHASE_DETAIL.PRODUCT_PUR_PRODUCT_CODE for SUB_CATEGORY */
    begin
      select PRODUCT_PUR_PRODUCT_CODE into PA_PURCHASE_tab(i).SUB_CATEGORY from       PA_PURCHASE_DETAIL 
      where PUR_PUR_ID=PA_PURCHASE_tab(i).PUR_ID
            and rownum<=1;
      exception 
        when others then null;
        PA_PURCHASE_tab(i).SUB_CATEGORY:=null;
    end;

    begin
      select PRODUCT_PUR_PRODUCT_CODE into PA_PURCHASE_tab(i).CATEGORY from PA_PURCHASE_DETAIL 
      where PUR_PUR_ID=PA_PURCHASE_tab(i).PUR_ID
            and rownum<=1;
      exception 
        when others then null;
        PA_PURCHASE_tab(i).CATEGORY:=null;
    end;

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_PAYMT_ITM_INFO_tab.first .. DM_PAYMT_ITM_INFO_tab.last
           INSERT INTO DM_PAYMT_ITM_INFO VALUES DM_PAYMT_ITM_INFO_tab(i);
                       
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


