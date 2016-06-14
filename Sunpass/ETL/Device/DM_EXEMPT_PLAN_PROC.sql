/********************************************************
*
* Name: DM_EXEMPT_PLAN_PROC
* Created by: DT, 4/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_EXEMPT_PLAN
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_EXEMPT_PLAN_PROC IS

TYPE DM_EXEMPT_PLAN_TYP IS TABLE OF DM_EXEMPT_PLAN%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_EXEMPT_PLAN_tab DM_EXEMPT_PLAN_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    INVTRANSP_TRANSP_TRANSP_ID ACCOUNT_NUMBER
    ,NULL PLAN_NAME
    ,NULL DEVICE_NUMBER
    ,NULL START_DATE
    ,NULL END_DATE
    ,'PAID' PLAN_STATUS
    ,'N' AUTO_RENEW
    ,NULL CREATED
    ,'000000' CREATED_BY
    ,NULL LAST_UPD
    ,'000000' LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
    ,NULL PLATE_STATE
    ,'..' PLATE_COUNTRY
    ,NULL PLATE_NUMBER
FROM PA_ACCT_TRANSP;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_EXEMPT_PLAN_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */
	
	
    FOR i in 1 .. DM_EXEMPT_PLAN_tab.count loop

    /* get EXEMPTIONS.EXEMPT_TYPE for PLAN_NAME */
    begin
      select EXEPMT_TYPE into DM_EXEMPT_PLAN_tab(i).PLAN_NAME from EXEMPTIONS 
      where TRANSPONDER_ID=DM_EXEMPT_PLAN_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_EXEMPT_PLAN_tab(i).PLAN_NAME:=null;
    end;

    /* get EXEMPTIONS.TRANSPONDER_ID for DEVICE_NUMBER */
    begin
      select TRANSPONDER_ID into DM_EXEMPT_PLAN_tab(i).DEVICE_NUMBER from EXEMPTIONS 
      where TRANSPONDER_ID=DM_EXEMPT_PLAN_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_EXEMPT_PLAN_tab(i).DEVICE_NUMBER:=null;
    end;

    /* get EXEMPTIONS.FROMDATE for START_DATE */
    begin
      select FROMDATE into DM_EXEMPT_PLAN_tab(i).START_DATE from EXEMPTIONS 
      where TRANSPONDER_ID=DM_EXEMPT_PLAN_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_EXEMPT_PLAN_tab(i).START_DATE:=null;
    end;

    /* get EXEMPTIONS.TODATE for END_DATE */
    begin
      select TODATE into DM_EXEMPT_PLAN_tab(i).END_DATE from EXEMPTIONS 
      where TRANSPONDER_ID=DM_EXEMPT_PLAN_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_EXEMPT_PLAN_tab(i).END_DATE:=null;
    end;

    /* get EXEMPTIONS.FROMDATE for CREATED */
    begin
      select FROMDATE into DM_EXEMPT_PLAN_tab(i).CREATED from EXEMPTIONS 
      where TRANSPONDER_ID=DM_EXEMPT_PLAN_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_EXEMPT_PLAN_tab(i).CREATED:=null;
    end;

    /* get EXEMPTIONS.FROMDATE for LAST_UPD */
    begin
      select FROMDATE into DM_EXEMPT_PLAN_tab(i).LAST_UPD from EXEMPTIONS 
      where TRANSPONDER_ID=DM_EXEMPT_PLAN_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_EXEMPT_PLAN_tab(i).LAST_UPD:=null;
    end;

    /* get EXEMPTIONS.JURISDICTION for PLATE_STATE */
    begin
      select JURISDICTION into DM_EXEMPT_PLAN_tab(i).PLATE_STATE from EXEMPTIONS 
      where TRANSPONDER_ID=DM_EXEMPT_PLAN_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_EXEMPT_PLAN_tab(i).PLATE_STATE:=null;
    end;

    /* get EXEMPTIONS.PLATE for PLATE_NUMBER */
    begin
      select PLATE into DM_EXEMPT_PLAN_tab(i).PLATE_NUMBER from EXEMPTIONS 
      where TRANSPONDER_ID=DM_EXEMPT_PLAN_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_EXEMPT_PLAN_tab(i).PLATE_NUMBER:=null;
    end;

    end loop;



    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_EXEMPT_PLAN_tab.first .. DM_EXEMPT_PLAN_tab.last
           INSERT INTO DM_EXEMPT_PLAN VALUES DM_EXEMPT_PLAN_tab(i);
                       
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


