/********************************************************
*
* Name: DM_RETAILER_CONTACT_INFO_PROC
* Created by: DT, 4/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_RETAILER_CONTACT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_RETAILER_CONTACT_INFO_PROC IS

TYPE DM_RETAILER_CONTACT_INFO_TYP IS TABLE OF DM_RETAILER_CONTACT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_RETAILER_CONTACT_INFO_tab DM_RETAILER_CONTACT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,'Y' IS_PRIMARY
    ,TITLE_TITLE_CODE TITLE
    ,F_NAME FIRST_NAME
    ,L_NAME LAST_NAME
    ,M_INITIAL MIDDLE_NAME
    ,DAY_PHONE||' ' ||DAY_PHONE_EXT PHONE_NUMBER_DAYTIME
    ,EVE_PHONE||' '|| EVE_PHONE_EXT PHONE_NUMBER_NIGHT
    ,NULL PHONE_NUMBER_MOBILE
    ,NULL FAX_NUMBER
    ,NULL DOB
    ,NULL GENDER
    ,DR_LIC_NUM DRIVER_LIC_NUMBER
    ,NULL DRIVER_LIC_EXP_DT
    ,DR_STATE_CODE DRIVER_LIC_STATE
    ,'fromST' DRIVER_LIC_COUNTRY
    ,ACCT_OPEN_DATE CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,to_date('02/27/2017','MM/DD/YYYY') LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM SUNPASS.PA_ACCT;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_RETAILER_CONTACT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_RETAILER_CONTACT_INFO_tab.first .. DM_RETAILER_CONTACT_INFO_tab.last
           INSERT INTO DM_RETAILER_CONTACT_INFO VALUES DM_RETAILER_CONTACT_INFO_tab(i);
                       
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


