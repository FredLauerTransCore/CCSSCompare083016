/********************************************************
*
* Name: DM_ADDRESS_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ADDRESS_INFO
*  Need the following info:
*  1. How to get OPEN_DATE for CREATED date
*  2. How to get SUNPASS_CSC_ID for CREATED_BY and LAST_UPD_BY
*  3. Get the COUNTY_CODE from FTE for COUNTY_CODE
*
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_ADDRESS_INFO_PROC IS

TYPE DM_ADDRESS_INFO_TYP IS TABLE OF DM_ADDRESS_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ADDRESS_INFO_tab DM_ADDRESS_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,NULL ADDR_TYPE
    ,decode('MAILING',1,
     'BILLING',2,
      'SHIPPING',3,
      'RESIDENT',4,
      'VIOLADDR',5,
     0) ADDR_TYPE_INT_ID
    ,ADDR_1 STREET_1
    ,ADDR_2 STREET_2
    ,CITY CITY
    ,STATE_STATE_CODE_ABBR STATE
    ,ZIP_CODE ZIP_CODE
    ,SUBSTR(ZIP_CODE,6,5) ZIP_PLUS4
    ,COUNTRY_COUNTRY_CODE COUNTRY
    ,RETURNED_MAIL_FLAG NIXIE
    ,to_date('02/02/2017','MM/DD/YYYY') NIXIE_DATE
    ,'N' NCOA_FLAG
    ,'N' ADDRESS_CLEANSED_FLG
    ,'CSC' ADDRESS_SOURCE
    ,NULL CREATED
    ,NULL CREATED_BY
    ,to_date('02/02/2017','MM/DD/YYYY') LAST_UPD
    ,NULL LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
    ,NULL ADDRESS_NUMBER
    ,f(COUNTY_CODE, 'Provied by FTE') COUNTY_CODE
FROM PATRON.PA_ACCT; 

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ADDRESS_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ADDRESS_INFO_tab.first .. DM_ADDRESS_INFO_tab.last
           INSERT INTO DM_ADDRESS_INFO VALUES DM_ADDRESS_INFO_tab(i);
                       
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


