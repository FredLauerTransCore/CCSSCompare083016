/********************************************************
*
* Name: DM_DEVICE_INFO_PROC
* Created by: DT, 4/19/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_DEVICE_INFO

We need more info

1. How to get BOX_NUMBER from PA_INV_TRANSP_ASSGN table /w TRAY_TRAY_NUM
2. How to get TRAY_NUMBER from PA_INV_TRANSP_ASSGN FTE table and TRAY_TRAY_NUM
3. How to get TRAY_CASE_CASE_NUM from PA_INV_TRANSP_ASSGN FTE table and TRAY_TRAY_NUM
4. Need IAG_CODE from Xerox



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
    , null BOX_NUMBER 
    ,subtsr(ACCT_ACCT_NUM AGENCY,9,2) AGENCY 
    ,to_date('12/31/9999','MM/DD/YYYY') EXPIRY_DATE 
    ,ISSUE_DATE DOB 
    ,VEHCLASS_VEH_CLASS_CODE VEHICLE_CLASS 
    ,'0' CLASS_MISMATCH_FLAG
    ,STATUS_POST_DATE STATUS_DATE 
    ,TRANSPSTAT_TRANSP_STATUS_CODE 
    ,NULL SWAP_PREV_NUMBER
    ,NULL SWAP_NEXT_NUMBER 
    ,NULL SWAP_DATE 
    ,'SOLD' ISSUE_TYPE 
    ,SALE_AMT DEVICE_PRICE 
    ,PREPAID_AMT DEVICE_DEPOSIT 
    ,NULL ASSIGNED_DATE 
    ,ISSUE_LOCATION STORE 
    ,WEB_REF_SEQ RETAILER_ACCOUNT_NUMBER 
    ,NULL VALIDATION_CODE 
    ,NULL PO_NUMBER 
    ,NULL LP_CHECK_REQUIRED 
    ,NULL PLATE_NUMBER 
    ,NULL PLATE_ASSOCIATED_DT 
    ,NULL UNREG_ACCOUNT_NUMBER 
    ,NULL CREATED 
    ,NULL CREATED_BY 
    ,NULL LAST_UPD 
    ,NULL LAST_UPD_BY 
    ,NULL TRAY_NUMBER 
    ,NULL CASE_NUMBER 
    ,SALE_DATE MANUF_WARRANTY_DT 
    ,NULL CUST_WARRANTY_DT 
    ,NULL MANUFACTURER 
    ,null RETAILER_NAME 
    ,NULL DEVICE_INTERNAL_NUMBER 
    ,null MCOMMERCE_FLAG 
    ,null FRIENDLY_NAME 
    ,'Need values from Xerox' IAG_CODE 
    ,null LOT 
    ,null TERMINATED_STATUS_FLAG 
    ,'SUNPASS' SOURCE_SYSTEM 
FROM PATRON.PA_ACCT_TRANSP;

BEGIN
 
 OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_DEVICE_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */
    for i in DM_DEVICE_INFO_tab.first .. DM_DEVICE_INFO_tab.last

    /* get PA_INV_TRANSP_ASSGN.TRAY_TRAY_NUM for BOX_NUMBER */
    begin
      select TRAY_TRAY_NUM into DM_DEVICE_INFO_tab(i).BOX_NUMBER from PA_INV_TRANSP_ASSGN where ACCT_ACCT_NUM=DM_DEVICE_INFO_tab(i).ACCT_ACCT_NUM;
      exception when others then null;
      DM_DEVICE_INFO_tab(i).BOX_NUMBER:=null;
    end;

         /* get PA_ACCT.ORG for RETAILER_NAME */
        begin
          select ORG into DM_DEVICE_INFO_tab(i).RETAILER_NAME from PA_ACCT where ACCT_ACCT_NUM=DM_DEVICE_INFO_tab(i).ACCT_NUM;
          exception when others then null;
          DM_DEVICE_INFO_tab(i).RETAILER_NAME:=null;
        end;
      
        /* get PA_ACCT_TRANSP.APPRKG_FLAG for MCOMMERCE_FLAG */
        begin
          select APPRKG_FLAG into DM_DEVICE_INFO_tab(i).MCOMMERCE_FLAG from PA_ACCT_TRANSP where ACCT_ACCT_NUM=DM_DEVICE_INFO_tab(i).ACCT_NUM;
          exception when others then null;
          DM_DEVICE_INFO_tab(i).MCOMMERCE_FLAG:=null;
        end;
      
        /* get PA_ACCT_TRANSP.FRIENDLY_NAME for FRIENDLY_NAME */
        begin
          select FRIENDLY_NAME into DM_DEVICE_INFO_tab(i).FRIENDLY_NAME from PA_ACCT_TRANSP where ACCT_ACCT_NUM=DM_DEVICE_INFO_tab(i).ACCT_NUM;
          exception when others then null;
          DM_DEVICE_INFO_tab(i).FRIENDLY_NAME:=null;
        end;
      
        /* get PA_INV_TRANSP.LOT_NUM for LOT */
        begin
          select LOT_NUM into DM_DEVICE_INFO_tab(i).LOT from PA_INV_TRANSP where ACCT_ACCT_NUM=DM_DEVICE_INFO_tab(i).ACCT_NUM;
          exception when others then null;
          DM_DEVICE_INFO_tab(i).LOT:=null;
        end;
      
        /* get PA_ACCT_TRANSP.BATCH_AFFECTED for TERMINATED_STATUS_FLAG */
        begin
          select BATCH_AFFECTED into DM_DEVICE_INFO_tab(i).TERMINATED_STATUS_FLAG from PA_ACCT_TRANSP where ACCT_ACCT_NUM=DM_DEVICE_INFO_tab(i).ACCT_NUM;
          exception when others then null;
          DM_DEVICE_INFO_tab(i).TERMINATED_STATUS_FLAG:=null;
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


