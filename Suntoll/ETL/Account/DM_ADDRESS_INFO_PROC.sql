/********************************************************
*
* Name: DM_ADDRESS_INFO_PROC
* Created by: RH, 5/16/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ADDRESS_INFO
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
    paa.ACCT_NUM ACCOUNT_NUMBER
    ,'MAILING' ADDR_TYPE
    ,'1' ADDR_TYPE_INT_ID
    ,paa.ADDR1 STREET_1    -- ADDR_1 mapping
    ,paa.ADDR2 STREET_2    -- ADDR_2 mapping
    ,paa.CITY CITY
    ,paa.STATE_CODE_ABBR STATE    -- STATE_STATE_CODE_ABBR mapping
    ,paa.ZIP_CODE ZIP_CODE
    ,SUBSTR(paa.ZIP_CODE,6,10) ZIP_PLUS4
    ,paa.COUNTRY_CODE COUNTRY   -- COUNTRY_COUNTRY_CODE mapping
    ,paa.BAD_ADDR_DATE NIXIE
    ,to_date('02/27/2017', 'MM/DD/YYYY') NIXIE_DATE
    ,'N' NCOA_FLAG
    ,'N' ADDRESS_CLEANSED_FLG
    ,'CSC' ADDRESS_SOURCE
    ,pa.ACCT_OPEN_DATE CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,to_date('02/27/2017', 'MM/DD/YYYY') LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
    ,NULL ADDRESS_NUMBER  -- DECODE?
    ,(select csl.COUNTY from COUNTY_STATE_LOOKUP csl
        where csl.CITY = pa.CITY)
        COUNTY_CODE
FROM PATRON.PA_ACCT_ADDR paa
    ,PATRON.PA_ACCT pa
WHERE paa.ACCT_NUM = pa.ACCT_NUM; -- Source

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


