/********************************************************
*
* Name: DM_CONTACT_INFO_PROC
* Created by: RH, 5/19/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_CONTACT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- EXTRACT RULE FOR ROV = 
-- Join ACCT_NUM of PA_ACCT to ACCT_NUM of EVENT_OWNER_ADDR_VEHICLE_ACCT 
--  AND OWNER_ID   of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_OWNER 
--  AND ADDRESS_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_ADDRESS 
--  AND VEHICLE_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_VEHICLE
DBMS_OUTPUT.PUT_LINE('Start load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

declare
--CREATE OR REPLACE PROCEDURE DM_CONTACT_INFO_PROC IS

TYPE DM_CONTACT_INFO_TYP IS TABLE OF DM_CONTACT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_CONTACT_INFO_tab DM_CONTACT_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,'Y' IS_PRIMARY
    ,TITLE_TITLE_CODE TITLE
    ,trim(nvl(F_NAME,'None')) FIRST_NAME
    ,trim(nvl(L_NAME,'None')) LAST_NAME
    ,trim(M_INITIAL) MIDDLE_NAME
    ,nvl(DAY_PHONE,'None')||DAY_PHONE_EXT PHONE_NUMBER_DAYTIME
    ,EVE_PHONE||EVE_PHONE_EXT PHONE_NUMBER_NIGHT
    ,NULL PHONE_NUMBER_MOBILE
    ,NULL FAX_NUMBER
    ,(select to_date(eo.DOB,'YYYYMMDD')
        from EVENT_OWNER eo,
             EVENT_OWNER_ADDR_VEHICLE_ACCT e
        where eo.ID = e.OWNER_ID
          and e.ACCT_NUM = pa.ACCT_NUM
          and rownum=1
          )  DOB
    ,NULL GENDER
    ,DR_LIC_NUM DRIVER_LIC_NUMBER
    ,NULL DRIVER_LIC_EXP_DT
    ,DR_STATE_CODE DRIVER_LIC_STATE
    ,(select nvl(cs.COUNTRY,'USA') from  COUNTRY_STATE_LOOKUP cs
        where cs.STATE_ABBR = DR_STATE_CODE) DRIVER_LIC_COUNTRY
    ,CREATED_ON CREATED -- CREATED_ON ?
    ,'SUNTOLL_CSC_ID' CREATED_BY
    ,to_date('27-FEB-2017','DD-MON-YYYY') LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PATRON.PA_ACCT pa
where rownum<10001
and L_NAME is null
; -- Source

SQL_STRING  varchar2(500) := 'truncate table ';
LOAD_TAB    varchar2(50) := 'DM_CONTACT_INFO';
ROW_CNT NUMBER := 0;

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  SQL_STRING := SQL_STRING||LOAD_TAB;
  DBMS_OUTPUT.PUT_LINE('SQL_STRING : '||SQL_STRING);
  execute immediate SQL_STRING;
  commit;
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_CONTACT_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_CONTACT_INFO_tab.first .. DM_CONTACT_INFO_tab.last
           INSERT INTO DM_CONTACT_INFO VALUES DM_CONTACT_INFO_tab(i);
                       
--    DBMS_OUTPUT.PUT_LINE('Inserted '||sql%rowcount||' into '||LOAD_TAB||' at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
    ROW_CNT := ROW_CNT +  sql%rowcount;                 
    DBMS_OUTPUT.PUT_LINE('Inserted ROW count : '||ROW_CNT);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;

  COMMIT;

  CLOSE C1;

  COMMIT;
  DBMS_OUTPUT.PUT_LINE('End '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total Rows : '||ROW_CNT);

  EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('ERROR on '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
END;
/
SHOW ERRORS


