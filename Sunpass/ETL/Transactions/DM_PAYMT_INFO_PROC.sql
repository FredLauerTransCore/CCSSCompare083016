/********************************************************
*
* Name: DM_PAYMT_INFO_PROC
* Created by: DT, 4/19/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PAYMT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PAYMT_INFO_PROC IS

TYPE DM_PAYMT_INFO_TYP IS TABLE OF DM_PAYMT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PAYMT_INFO_tab DM_PAYMT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,PUR_TRANS_DATE TX_DT
    ,NULL PAY_TYPE  
    ,NULL TRAN_TYPE
    ,NULL AMOUNT 
    ,NULL REVERSED 
    ,NULL EXP_MONTH
    ,NULL EXP_YEAR 
    ,NULL CREDIT_CARD_NUMBER 
    ,NULL BANK_ACCOUNT_NUMBER 
    ,NULL BANK_ROUTING_NUMBER 
    ,NULL CHECK_NUMBER  
    ,NULL NSF_FEE
    ,PUR_ID PAYMENT_REFERENCE_NUM
    ,NULL EMPLOYEE_NUMBER 
    ,PUR_ID TRANSACTION_ID 
    ,NULL  ORG_TRANSACTION_ID
    ,NULL XREF_TRANSACTION_ID 
    ,NULL CC_RESPONSE_CODE
    ,NULL EXTERNAL_REFERENCE_NUM
    ,NULL MISS_APPLIED
    ,NULL DESCRIPTION
    ,NULL REFUND_STATUS
    ,NULL REFUND_CHECK_NUM
    ,NULL TOD_ID
    ,PUR_TRANS_DATE CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,NULL LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
    ,NULL CC_GATEWAY_REQ_ID
    ,NULL CC_TXN_REF_NUM
FROM PA_PURCHASE;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PAYMT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    for i in 1 .. DM_PAYMT_INFO_tab.count loop
 
    /* get PA_PURCHASE_DETAIL.PRODUCT_PUR_PRODUCT_CODE for TRAN_TYPE */
    begin
      select PRODUCT_PUR_PRODUCT_CODE , PRODUCT_PUR_PRODUCT_CODE ,
	  PRODUCT_PUR_PRODUCT_CODE ,VES_REF_NUM 
	  into 
	  DM_PAYMT_INFO_tab(i).TRAN_TYPE ,
	  DM_PAYMT_INFO_tab(i).REVERSED,
	  DM_PAYMT_INFO_tab(i).NSF_FEE,
	  DM_PAYMT_INFO_tab(i).ORG_TRANSACTION_ID
	  from PA_PURCHASE_DETAIL 
      where PUR_PUR_ID =DM_PAYMT_INFO_tab(i).PAYMENT_REFERENCE_NUM;
      exception when others then null;
      DM_PAYMT_INFO_tab(i).TRAN_TYPE:='R';
    end;

  
  

  
    /* get PA_PURCHASE_PAYMENT.CC_GATEWAY_REQ_ID for CC_GATEWAY_REQ_ID */
    begin
      select CC_GATEWAY_REQ_ID, CC_TXN_REF_NUM ,
	  EMP_EMP_CODE, to_char(PUR_CREDIT_EXP_DATE,'YYYY') ,
	  EMP_EMP_CODE, to_char(PUR_CREDIT_EXP_DATE,'MM') ,
	  PUR_PAY_AMT  , PAYTYPE_PAYMENT_TYPE_CODE 
	  into 
	  DM_PAYMT_INFO_tab(i).CC_GATEWAY_REQ_ID , 
	  DM_PAYMT_INFO_tab(i).CC_TXN_REF_NUM,
	  DM_PAYMT_INFO_tab(i).EMPLOYEE_NUMBER,
	  DM_PAYMT_INFO_tab(i).EXP_YEAR,
	  DM_PAYMT_INFO_tab(i).CREATED_BY,
	  DM_PAYMT_INFO_tab(i).EXP_MONTH,
	  DM_PAYMT_INFO_tab(i).AMOUNT,
	  DM_PAYMT_INFO_tab(i).PAY_TYPE
	  from PA_PURCHASE_PAYMENT 
	  where PUR_PUR_ID=DM_PAYMT_INFO_tab(i).PAYMENT_REFERENCE_NUM;
      exception when others then null;
      DM_PAYMT_INFO_tab(i).CC_GATEWAY_REQ_ID:=null;
    end;



    end loop;
	
	
	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_PAYMT_INFO_tab.count loop
	 if DM_PAYMT_INFO_tab(i).ACCOUNT_NUMBER is null then
          DM_PAYMT_INFO_tab(i).ACCOUNT_NUMBER:='0';
         end if;
	 if DM_PAYMT_INFO_tab(i).TX_DT is null then
          DM_PAYMT_INFO_tab(i).TX_DT:=sysdate;
         end if;
	 if DM_PAYMT_INFO_tab(i).PAY_TYPE is null then
          DM_PAYMT_INFO_tab(i).PAY_TYPE:='0';
         end if;
	 if DM_PAYMT_INFO_tab(i).AMOUNT is null then
          DM_PAYMT_INFO_tab(i).AMOUNT:='0';
         end if;
	 if DM_PAYMT_INFO_tab(i).PAYMENT_REFERENCE_NUM is null then
          DM_PAYMT_INFO_tab(i).PAYMENT_REFERENCE_NUM:='0';
         end if;
	 if DM_PAYMT_INFO_tab(i).TRANSACTION_ID is null then
          DM_PAYMT_INFO_tab(i).TRANSACTION_ID:='0';
         end if;
	 if DM_PAYMT_INFO_tab(i).TOD_ID is null then
          DM_PAYMT_INFO_tab(i).TOD_ID:='0';
         end if;
	 if DM_PAYMT_INFO_tab(i).CREATED is null then
          DM_PAYMT_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_PAYMT_INFO_tab(i).LAST_UPD is null then
          DM_PAYMT_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_PAYMT_INFO_tab.first .. DM_PAYMT_INFO_tab.last
           INSERT INTO DM_PAYMT_INFO VALUES DM_PAYMT_INFO_tab(i);
                       
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


