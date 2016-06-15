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


CREATE OR REPLACE PROCEDURE DM_ADDRESS_INFO_PROC 
--  (io_trac_rec IN OUT dm_tracking_etl%ROWTYPE)
  (io_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_ADDRESS_INFO_TYP IS TABLE OF DM_ADDRESS_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ADDRESS_INFO_tab DM_ADDRESS_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1(p_begin_acct_num  pa_acct.acct_num%TYPE, 
          p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    paa.ACCT_NUM ACCOUNT_NUMBER
    ,'MAILING' ADDR_TYPE
    ,'1' ADDR_TYPE_INT_ID
    ,paa.ADDR1 STREET_1    -- ADDR_1 mapping
    ,paa.ADDR2 STREET_2    -- ADDR_2 mapping
    ,paa.CITY CITY
    ,paa.STATE_CODE_ABBR STATE    -- STATE_STATE_CODE_ABBR mapping
    ,paa.ZIP_CODE ZIP_CODE
    ,SUBSTR(paa.ZIP_CODE,7,10) ZIP_PLUS4
    ,paa.COUNTRY_CODE COUNTRY   -- COUNTRY_COUNTRY_CODE mapping
    ,paa.BAD_ADDR_DATE NIXIE
    ,to_date('02/27/2017', 'MM/DD/YYYY') NIXIE_DATE
    ,'N' NCOA_FLAG
    ,'N' ADDRESS_CLEANSED_FLG
    ,'CSC' ADDRESS_SOURCE
    ,pa.CREATED_ON CREATED
    ,'SUNTOLL_CSC_ID' CREATED_BY
    ,to_date('02/27/2017', 'MM/DD/YYYY') LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
    ,NULL ADDRESS_NUMBER  -- DECODE?
--    ,(select csl.COUNTY from COUNTY_STATE_LOOKUP csl
--        where csl.CITY = pa.CITY) COUNTY_CODE
    ,NULL     COUNTY_CODE
FROM PA_ACCT_ADDR paa
    ,PA_ACCT pa
WHERE paa.ACCT_NUM = pa.ACCT_NUM;

v_begin_acct  dm_tracking.begin_acct%TYPE;
v_end_acct    dm_tracking.end_acct%TYPE;
row_cnt NUMBER := 0;
v_trac_rec dm_tracking_etl%ROWTYPE;

BEGIN
  select * into v_trac_rec
  from dm_tracking_etl
  where track_etl_id = io_trac_id;
  DBMS_OUTPUT.PUT_LINE('Start '||v_trac_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
 
--  update_track_proc(io_trac_rec);
  update_track_proc(v_trac_rec);
 
  SELECT begin_acct, end_acct
  INTO   v_begin_acct, v_end_acct
  FROM   dm_tracking
  WHERE  track_id = v_trac_rec.track_id
  ;
  
  OPEN C1(v_begin_acct,v_end_acct);  
  v_trac_rec.status := 'Processing';

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ADDRESS_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ADDRESS_INFO_tab.first .. DM_ADDRESS_INFO_tab.last
           INSERT INTO DM_ADDRESS_INFO VALUES DM_ADDRESS_INFO_tab(i);

    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_rec);
                                           
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total ROW_CNT : '||ROW_CNT);

  COMMIT;

  CLOSE C1;

  COMMIT;

  EXCEPTION
  WHEN OTHERS THEN
    v_trac_rec.result_code := SQLCODE;
    v_trac_rec.result_msg := SQLERRM;
    update_track_proc(v_trac_rec);
--     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_rec.result_code);
--     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_rec.result_msg);
END;
/
SHOW ERRORS


