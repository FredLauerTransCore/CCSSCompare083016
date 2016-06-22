/********************************************************
*
* Name: DM_DEVICE_INFO_PROC
* Created by: DT, 4/22/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_DEVICE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_DEVICE_INFO_PROC IS

TYPE DM_DEVICE_INFO_TYP IS TABLE OF DM_DEVICE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_DEVICE_INFO_tab DM_DEVICE_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,TRANSP_SEQ_NUM DEVICE_NUMBER
    ,TRANSPTYPE_TRANSP_TYPE_CODE DEVICE_MODEL
    ,NULL BOX_NUMBER
    ,SUBSTR(TRANSP_SEQ_NUM,9,2) AGENCY
    ,to_date('12/31/9999','MM/DD/YYYY') EXPIRY_DATE
    ,ISSUE_DATE DOB
    ,VEHCLASS_VEH_CLASS_CODE VEHICLE_CLASS
    ,'0' CLASS_MISMATCH_FLAG
    ,NULL STATUS_DATE
    ,TRANSPSTAT_TRANSP_STATUS_CODE DEVICE_STATUS
    ,NULL SWAP_PREV_NUMBER
    ,NULL SWAP_NEXT_NUMBER
    ,NULL SWAP_DATE
    ,'SOLD' ISSUE_TYPE
    ,SALE_AMT DEVICE_PRICE
    ,NULL DEVICE_DEPOSIT
    ,SALE_DATE ASSIGNED_DATE
    ,NULL STORE
    ,WEB_REF_SEQ RETAILER_ACCOUNT_NUMBER
    ,NULL VALIDATION_CODE
    ,NULL PO_NUMBER
    ,NULL LP_CHECK_REQUIRED
    ,NULL PLATE_NUMBER
    ,NULL PLATE_ASSOCIATED_DT
    ,NULL UNREG_ACCOUNT_NUMBER
    ,NULL CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,NULL LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,NULL TRAY_NUMBER
    ,NULL CASE_NUMBER
    ,SALE_DATE MANUF_WARRANTY_DT
    ,SALE_DATE CUST_WARRANTY_DT
    ,'TRANSCORE' MANUFACTURER
    ,NULL RETAILER_NAME
    ,NULL DEVICE_INTERNAL_NUMBER
    ,APPRKG_FLAG MCOMMERCE_FLAG
    ,FRIENDLY_NAME FRIENDLY_NAME
    ,2 IAG_CODE
    ,NULL LOT
    ,BATCH_AFFECTED TERMINATED_STATUS_FLAG
    ,'SUNPASS' SOURCE_SYSTEM
	, NULL DEVICE_STATUS_INT_ID
FROM PA_ACCT_TRANSP;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_DEVICE_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in DM_DEVICE_INFO_tab.first .. DM_DEVICE_INFO_tab.last loop

    /* get PA_ACCT.ORG for RETAILER_NAME */
    begin
      select ORG into DM_DEVICE_INFO_tab(i).RETAILER_NAME from PA_ACCT 
      where ACCT_NUM=DM_DEVICE_INFO_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_DEVICE_INFO_tab(i).RETAILER_NAME:=null;
    end;
    
    /* get PA_INV_TRANSP.TRANSP_SOURCE for TRANSP_SOURCE */
    begin
      select TRANSP_SOURCE,TRAY_TRAY_NUM,TRAY_TRAY_NUM,TRAY_CASE_CASE_NUM
      into DM_DEVICE_INFO_tab(i).MANUFACTURER,DM_DEVICE_INFO_tab(i).BOX_NUMBER,DM_DEVICE_INFO_tab(i).TRAY_NUMBER,DM_DEVICE_INFO_tab(i).CASE_NUMBER
      from PA_INV_TRANSP 
      where INVTRANSP_TRANSP_ID=DM_DEVICE_INFO_tab(i).DEVICE_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_DEVICE_INFO_tab(i).MANUFACTURER:=null;
        DM_DEVICE_INFO_tab(i).BOX_NUMBER:=null;
        DM_DEVICE_INFO_tab(i).TRAY_NUMBER:=null;
        DM_DEVICE_INFO_tab(i).CASE_NUMBER:=null;
    end;

    end loop;

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_DEVICE_INFO_tab.first .. DM_DEVICE_INFO_tab.last
           INSERT INTO DM_DEVICE_INFO VALUES DM_DEVICE_INFO_tab(i);
                       
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


