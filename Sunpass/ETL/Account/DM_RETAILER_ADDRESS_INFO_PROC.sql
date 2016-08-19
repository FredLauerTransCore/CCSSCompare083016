/********************************************************
*
* Name: DM_RETAILER_ADDRESS_INFO_PROC
* Created by: DT, 4/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_RETAILER_ADDRESS_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_RETAILER_ADDRESS_INFO_PROC IS

TYPE DM_RETAILER_ADDRESS_INFO_TYP IS TABLE OF DM_RETAILER_ADDRESS_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_RETAILER_ADDRESS_INFO_tab DM_RETAILER_ADDRESS_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,'MAILING' ADDR_TYPE
    ,'1' ADDR_TYPE_INT_ID
    ,ADDR_1 STREET_1
    ,ADDR_2 STREET_2
    ,CITY CITY
    ,STATE_STATE_CODE_ABBR STATE
    ,CASE
    WHEN zip_code LIKE '%-%'
    THEN SUBSTR(zip_code,1,5)
    ELSE
      CASE LENGTH(zip_code)
        WHEN 5 THEN zip_code
        WHEN 6 then upper(zip_code)
        ELSE SUBSTR(zip_code,1,5)
      END
    END zip_code
    ,SUBSTR(ZIP_CODE,7,4) ZIP_PLUS4
    ,COUNTRY_COUNTRY_CODE COUNTRY
    ,NULL NIXIE
    ,to_date('02/27/2017','MM/DD/YYYY') NIXIE_DATE
    ,'N' NCOA_FLAG
    ,'N' ADDRESS_CLEANSED_FLG
    ,'CSC' ADDRESS_SOURCE
    ,ACCT_OPEN_DATE CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,to_date('02/27/2017','MM/DD/YYYY') LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
    ,COUNTY_COUNTY_CODE COUNTY_CODE
FROM PA_ACCT where ACCTTYPE_ACCT_TYPE_CODE in ('07','08');

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_RETAILER_ADDRESS_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    FOR i in 1 .. DM_RETAILER_ADDRESS_INFO_tab.count loop

    /* get PA_ACCT_FLAGS.MAIL_RETURNED for NIXIE */
    begin
      select MAIL_RETURNED into DM_RETAILER_ADDRESS_INFO_tab(i).NIXIE from PA_ACCT_FLAGS 
      where ACCT_ACCT_NUM=DM_RETAILER_ADDRESS_INFO_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_RETAILER_ADDRESS_INFO_tab(i).NIXIE:=null;
    end;
	
	begin
      select COUNTRY into DM_RETAILER_ADDRESS_INFO_tab(i).COUNTRY from COUNTRY_STATE_LOOKUP 
      where STATE_ABBR=DM_RETAILER_ADDRESS_INFO_tab(i).STATE
            and rownum<=1;
      exception 
        when others then null;
        DM_RETAILER_ADDRESS_INFO_tab(i).COUNTRY:='USA';
    end;

    end loop;



    /*ETL SECTION END   */

	
     /* Bulk delete *****************************************/

     begin
       FORALL i in 1 .. DM_RETAILER_ADDRESS_INFO_tab.count
             delete DM_RETAILER_ADDRESS_INFO where  
              ACCOUNT_NUMBER=DM_RETAILER_ADDRESS_INFO_tab(i).ACCOUNT_NUMBER and 
              ADDR_TYPE=DM_RETAILER_ADDRESS_INFO_tab(i).ADDR_TYPE;
       exception 
       when others then null;
     end;

    /* Bulk delete *****************************************/
	
    /*Bulk insert */ 
    FORALL i in DM_RETAILER_ADDRESS_INFO_tab.first .. DM_RETAILER_ADDRESS_INFO_tab.last
           INSERT INTO DM_RETAILER_ADDRESS_INFO VALUES DM_RETAILER_ADDRESS_INFO_tab(i);
                       
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


