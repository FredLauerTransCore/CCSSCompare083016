/********************************************************
*
* Name: DM_CONTACT_INFO_PROC
* Created by: DT, 4/21/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_CONTACT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_CONTACT_INFO_PROC IS

TYPE DM_CONTACT_INFO_TYP IS TABLE OF DM_CONTACT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_CONTACT_INFO_tab DM_CONTACT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,'Y' IS_PRIMARY
    ,TITLE_TITLE_CODE TITLE
    ,F_NAME FIRST_NAME
    ,L_NAME LAST_NAME
    ,M_INITIAL MIDDLE_NAME
    ,DAY_PHONE PHONE_NUMBER_DAYTIME
    ,EVE_PHONE PHONE_NUMBER_NIGHT
    ,NULL PHONE_NUMBER_MOBILE
    ,NULL FAX_NUMBER
    ,NULL DOB
    ,NULL GENDER
    ,DR_LIC_NUM DRIVER_LIC_NUMBER
    ,to_date('01/01/1858', 'MM/DD/YYYY') DRIVER_LIC_EXP_DT
    ,DR_STATE_CODE DRIVER_LIC_STATE
    ,NULL DRIVER_LIC_COUNTRY
    ,ACCT_OPEN_DATE CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,to_date('02/27/2017','MM/DD/YYYY') LAST_UPD
    ,NULL LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_ACCT where ACCTTYPE_ACCT_TYPE_CODE not in ('07','08');

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_CONTACT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */
	 FOR i in 1 .. DM_CONTACT_INFO_tab.count loop
	 begin
      select COUNTRY into DM_CONTACT_INFO_tab(i).DRIVER_LIC_COUNTRY from COUNTRY_STATE_LOOKUP 
      where STATE_ABBR in (select STATE_STATE_CODE_ABBR from PA_ACCT where acct_num=DM_CONTACT_INFO_tab(i).account_number)
            and rownum<=1;
      exception 
        when others then null;
        DM_CONTACT_INFO_tab(i).DRIVER_LIC_COUNTRY:=null;
      end;
	  
	
    /* get pa_title_code.TITLE_DESC for TITLE */
    begin
      select rtrim(TITLE_DESC) into dm_contact_info_tab(i).TITLE from pa_title_code 
      where TITLE_CODE=dm_contact_info_tab(i).TITLE
            and rownum<=1;
      exception 
        when others then null;
        dm_contact_info_tab(i).TITLE:=null;
    end;

    /* to default the values NOT NULL columns */
 
	 if DM_CONTACT_INFO_tab(i).CREATED is null then
          DM_CONTACT_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_CONTACT_INFO_tab(i).LAST_UPD is null then
          DM_CONTACT_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
	 if DM_CONTACT_INFO_tab(i).ACCOUNT_NUMBER is null then
          DM_CONTACT_INFO_tab(i).ACCOUNT_NUMBER:='0';
         end if;
	 if DM_CONTACT_INFO_tab(i).IS_PRIMARY is null then
          DM_CONTACT_INFO_tab(i).IS_PRIMARY:='0';
         end if;
	 if DM_CONTACT_INFO_tab(i).FIRST_NAME is null then
          DM_CONTACT_INFO_tab(i).FIRST_NAME:='0';
         end if;
	 if DM_CONTACT_INFO_tab(i).LAST_NAME is null then
          DM_CONTACT_INFO_tab(i).LAST_NAME:='0';
         end if;
	 if DM_CONTACT_INFO_tab(i).PHONE_NUMBER_DAYTIME is null then
          DM_CONTACT_INFO_tab(i).PHONE_NUMBER_DAYTIME:='0';
         end if;
    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_CONTACT_INFO_tab.first .. DM_CONTACT_INFO_tab.last
           INSERT INTO DM_CONTACT_INFO VALUES DM_CONTACT_INFO_tab(i);
                       
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


