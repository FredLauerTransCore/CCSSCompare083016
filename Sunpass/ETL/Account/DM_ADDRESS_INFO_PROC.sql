/********************************************************
*
* Name: DM_ADDRESS_INFO_PROC
* Created by: DT, 4/21/2016
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
    ACCT_NUM ACCOUNT_NUMBER
    ,'MAILING' ADDR_TYPE
    ,NULL ADDR_TYPE_INT_ID
    ,ADDR_1 STREET_1
    ,ADDR_2 STREET_2
    ,CITY CITY
    ,STATE_STATE_CODE_ABBR STATE
    ,  CASE
    WHEN zip_code LIKE '%-%'
    THEN SUBSTR(zip_code,1,5)
    ELSE
      CASE LENGTH(zip_code)
        WHEN 5
        THEN zip_code
        WHEN 6 then upper(zip_code)
        ELSE SUBSTR(zip_code,1,5)
      END
  END zip_code,
  substr(zip_code,7,4) ZIP_PLUS4
    ,NULL COUNTRY
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
    ,NULL ADDRESS_NUMBER
    ,COUNTY_COUNTY_CODE COUNTY_CODE
FROM PA_ACCT where ACCTTYPE_ACCT_TYPE_CODE not in ('07','08');

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ADDRESS_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in 1 .. DM_address_info_tab.count loop

    /* get PA_ACCT_FLAGS.MAIL_RETURNED for NIXIE */
    begin
      select decode(MAIL_RETURNED,0,'Y',null,'Y','N') into DM_ADDRESS_INFO_tab(i).NIXIE from PA_ACCT_FLAGS 
      where ACCT_ACCT_NUM=DM_ADDRESS_INFO_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_ADDRESS_INFO_tab(i).NIXIE:='N';
    end;

	    /* get COUNTRY_STATE_LOOKUP.COUNTRY for COUNTRY */
    begin
      select COUNTRY into DM_ADDRESS_INFO_tab(i).COUNTRY from COUNTRY_STATE_LOOKUP 
      where STATE_ABBR in 
            (select STATE_STATE_CODE_ABBR from pa_acct where acct_num=DM_ADDRESS_INFO_tab(i).account_number)
            and rownum<=1;
      exception 
        when others then null;
        DM_ADDRESS_INFO_tab(i).COUNTRY:='USA';
		if DM_ADDRESS_INFO_tab(i).STATE is null then
	       DM_ADDRESS_INFO_tab(i).STATE:='FL';
	    end if;
    end;

    end loop;



    /*ETL SECTION END   */

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


