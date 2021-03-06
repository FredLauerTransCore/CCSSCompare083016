/********************************************************
*
* Name: DM_DEVICE_HST_INFO_PROC
* Created by: DT, 5/6/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_DEVICE_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_DEVICE_HST_INFO_PROC IS

TYPE DM_DEVICE_HST_INFO_TYP IS TABLE OF DM_DEVICE_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_DEVICE_HST_INFO_tab DM_DEVICE_HST_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    --,TRANSP_SEQ_NUM DEVICE_NUMBER
	,INVTRANSP_TRANSP_TRANSP_ID DEVICE_NUMBER
    ,TRANSPTYPE_TRANSP_TYPE_CODE DEVICE_MODEL
    ,NULL BOX_NUMBER
    ,NULL AGENCY
    ,to_date('12/31/9999','MM/DD/YYYY') EXPIRY_DATE
    ,NULL DOB
    ,VEHCLASS_VEH_CLASS_CODE VEHICLE_CLASS
    ,'N' CLASS_MISMATCH_FLAG
    ,NULL STATUS_DATE
    ,TRANSPSTAT_TRANSP_STATUS_CODE DEVICE_STATUS
    ,NULL SWAP_PREV_NUMBER
    ,NULL SWAP_NEXT_NUMBER
    ,NULL SWAP_DATE
    ,'SOLD' ISSUE_TYPE
    ,SALE_AMT DEVICE_PRICE
    ,NULL DEVICE_DEPOSIT
    ,SALE_DATE ASSIGNED_DATE
    ,NULL  STORE
    ,WEB_REF_SEQ RETAILER_ACCOUNT_NUMBER
    ,'0' VALIDATION_CODE
    ,NULL PO_NUMBER
    ,NULL LP_CHECK_REQUIRED
    ,NULL PLATE_NUMBER
    ,NULL PLATE_ASSOCIATED_DT
    ,NULL UNREG_ACCOUNT_NUMBER
    ,SALE_DATE CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,NULL TRAY_NUMBER
    ,NULL CASE_NUMBER
    ,SALE_DATE MANUF_WARRANTY_DT
    ,ADD_months(SALE_DATE,12) CUST_WARRANTY_DT
    ,NULL  MANUFACTURER
    ,NULL RETAILER_NAME
    ,NULL DEVICE_INTERNAL_NUMBER
    ,APPRKG_FLAG MCOMMERCE_FLAG
    ,FRIENDLY_NAME FRIENDLY_NAME
    ,'0' IAG_CODE
    ,'0' LOT
    ,'0' TERMINATED_STATUS_FLAG
    ,'SUNPASS' SOURCE_SYSTEM
    ,'0' ACTIVITY_ID
    ,NULL X_AGENCY_CLASS
    ,'0' X_ASSIGN_TYPE
    ,NULL X_COLOUR
    ,NULL X_DATE_RECEIVED_AFTER_LOST
    ,to_date('01/01/00','MM/DD/YY') X_TESTED_DATETIME
    ,NULL X_CHANGED_EMPLOYEE
    ,NULL X_IS_DEVICE_PREVALIDATED
    ,NULL X_DEVICE_CHAR_INTID
    ,NULL X_DEVICE_STATUS_INTID
    ,'0' X_SWAP_STATUS
    ,NULL X_DEVICE_SWAP_FLAG
    ,NULL X_ETC_ACCOUNT_ID
    ,'0' X_VERSION
    ,'0' X_PREVIOUS_STOREID
    ,'0' X_PREVIOUS_STATUS
    ,NULL X_PO_RECEIPTID
    ,'0' X_RETAIL_TAG_STATUS
    ,'0' X_RETURN_REASON
    ,'0' X_IS_FREE_TAG
    ,NULL X_RETURN_DATE
    ,'0' X_ORGID
    ,NULL X_REQUEST_MODE
    ,NULL X_LAST_UPD
    ,NULL X_MOUNT_TYPE
FROM PA_ACCT_TRANSP;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_DEVICE_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */



    FOR i in 1 .. DM_DEVICE_HST_INFO_tab.count loop

    /* get PA_INV_TRANSP.TRAY_TRAY_NUM for BOX_NUMBER */
    begin
      select TRAY_TRAY_NUM into DM_DEVICE_HST_INFO_tab(i).BOX_NUMBER from PA_INV_TRANSP 
      where INVTRANSP_TRANSP_ID=DM_DEVICE_HST_INFO_tab(i).DEVICE_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_DEVICE_HST_INFO_tab(i).BOX_NUMBER:=null;
    end;
    
    /* get PA_INV_TRANSP.TRANSP_SOURCE for MANUFACTURER */
    begin
      select TRANSP_SOURCE into DM_DEVICE_HST_INFO_tab(i).MANUFACTURER from PA_INV_TRANSP 
      where INVTRANSP_TRANSP_ID=DM_DEVICE_HST_INFO_tab(i).DEVICE_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_DEVICE_HST_INFO_tab(i).MANUFACTURER:='TRANSCORE';
    end;
	if DM_DEVICE_HST_INFO_tab(i).MANUFACTURER is null then
      DM_DEVICE_HST_INFO_tab(i).MANUFACTURER:='TRANSCORE';
	end if;
	
    /* get PA_INV_TRANSP.ISSUE_DATE for DOB */
    begin
      select ISSUE_DATE into DM_DEVICE_HST_INFO_tab(i).DOB from PA_INV_TRANSP 
      where INVTRANSP_TRANSP_ID=DM_DEVICE_HST_INFO_tab(i).DEVICE_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_DEVICE_HST_INFO_tab(i).DOB:=null;
    end;

    /* get PA_TRANS_STATUS_CHG_CODE.STATUS_POST_DATE for STATUS_DATE */
	/*
    begin
      select STATUS_POST_DATE into DM_DEVICE_HST_INFO_tab(i).STATUS_DATE from PA_TRANS_STATUS_CHG_CODE 
      where TRANSPSTATCHG_ID=DM_DEVICE_HST_INFO_tab(i).DEVICE_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_DEVICE_HST_INFO_tab(i).STATUS_DATE:=null;
    end;
    */
    /* get PA_INV_TRANSP_ASSGN.ISSUE_LOCATION for STORE */
    begin
      select ISSUE_LOCATION into DM_DEVICE_HST_INFO_tab(i).STORE from PA_INV_TRANSP_ASSGN 
      where TRAY_TRAY_NUM=DM_DEVICE_HST_INFO_tab(i).BOX_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_DEVICE_HST_INFO_tab(i).STORE:=null;
    end;

    /* get PA_ACCT.ORG for RETAILER_NAME */
    begin
      select ORG into DM_DEVICE_HST_INFO_tab(i).RETAILER_NAME from PA_ACCT 
      where ACCT_NUM=DM_DEVICE_HST_INFO_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_DEVICE_HST_INFO_tab(i).RETAILER_NAME:=null;
    end;

    /* get PA_INV_TRANSP.TRANSTYPE_TRANS_TYPE_CODE for X_MOUNT_TYPE */
    begin
      select TRANSPTYPE_TRANSP_TYPE_CODE into DM_DEVICE_HST_INFO_tab(i).X_MOUNT_TYPE from PA_INV_TRANSP 
      where INVTRANSP_TRANSP_ID=DM_DEVICE_HST_INFO_tab(i).DEVICE_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_DEVICE_HST_INFO_tab(i).X_MOUNT_TYPE:=null;
    end;

    end loop;





    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_DEVICE_HST_INFO_tab.first .. DM_DEVICE_HST_INFO_tab.last
           INSERT INTO DM_DEVICE_HST_INFO VALUES DM_DEVICE_HST_INFO_tab(i);
                       
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


