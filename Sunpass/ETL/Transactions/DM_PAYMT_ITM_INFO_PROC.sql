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

T_PRODUCT_PUR_PRODUCT_CODE varchar2(2);

CURSOR C1 IS SELECT 
    ppd.PRODUCT_PUR_PRODUCT_CODE CATEGORY
    ,ppd.PRODUCT_PUR_PRODUCT_CODE SUB_CATEGORY
    ,ppd.PRODUCT_PUR_PRODUCT_CODE TRANS_TYPE
    ,PROD_AMT AMOUNT
    ,'FTE' ENDO_AGENCY
    ,'FTE' REVENUE_AGENCY
    ,NULL DESCRIPTION
    ,NULL TRANSFER_REASON
    ,NULL NOTICE_NUMBER
    ,NULL SEQUENCE_NUMBER
    ,PUR_PUR_ID PARENT_PAYMENT_ID
    ,sysdate CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,sysdate LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,NULL INVOICE_NUM
    ,NULL INVOICE_REF_ID 
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_PURCHASE_DETAIL ppd;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PAYMT_ITM_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in 1 .. DM_PAYMT_ITM_INFO_tab.count loop
	
	
	 begin
      select PUR_TRANS_DATE, PUR_ID  into 
	  DM_PAYMT_ITM_INFO_tab(i).CREATED,
	  DM_PAYMT_ITM_INFO_tab(i).INVOICE_NUM from PA_PURCHASE 
      where PUR_ID=DM_PAYMT_ITM_INFO_tab(i).PARENT_PAYMENT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_PAYMT_ITM_INFO_tab(i).CREATED:=null;
    end;
	
	begin
      select EMP_EMP_CODE into DM_PAYMT_ITM_INFO_tab(i).CREATED_BY from PA_PURCHASE_PAYMENT       
	  where PUR_PAY_ID=DM_PAYMT_ITM_INFO_tab(i).PARENT_PAYMENT_ID and rownum<=1;
      exception 
	  when others then null;
        DM_PAYMT_ITM_INFO_tab(i).CREATED_BY:=null;
    end;
	
	 begin
      select PUR_PRODUCT_DESC into DM_PAYMT_ITM_INFO_tab(i).DESCRIPTION from PA_PUR_PRODUCT 
      where PUR_PRODUCT_CODE= DM_PAYMT_ITM_INFO_tab(i).CATEGORY 
            and rownum<=1;
      exception 
        when others then null;
        DM_PAYMT_ITM_INFO_tab(i).DESCRIPTION:=null;
    end;
	
	 /* to default the values NOT NULL columns */
 	 if DM_PAYMT_ITM_INFO_tab(i).CATEGORY is null then
          DM_PAYMT_ITM_INFO_tab(i).CATEGORY:='0';
         end if;
	 if DM_PAYMT_ITM_INFO_tab(i).SUB_CATEGORY is null then
          DM_PAYMT_ITM_INFO_tab(i).SUB_CATEGORY:='0';
         end if;
	 if DM_PAYMT_ITM_INFO_tab(i).TRANS_TYPE is null then
          DM_PAYMT_ITM_INFO_tab(i).TRANS_TYPE:='0';
         end if;
	 if DM_PAYMT_ITM_INFO_tab(i).AMOUNT is null then
          DM_PAYMT_ITM_INFO_tab(i).AMOUNT:='0';
         end if;
	 if DM_PAYMT_ITM_INFO_tab(i).PARENT_PAYMENT_ID is null then
          DM_PAYMT_ITM_INFO_tab(i).PARENT_PAYMENT_ID:='0';
         end if;
	 if DM_PAYMT_ITM_INFO_tab(i).CREATED is null then
          DM_PAYMT_ITM_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_PAYMT_ITM_INFO_tab(i).LAST_UPD is null then
          DM_PAYMT_ITM_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
    end loop;

	
	
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


