/********************************************************
*
* Name: DM_ADDRESS_HST_INFO_PROC
* Created by: RH, 5/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ADDRESS_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- RH 6/20/2016 Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_ADDRESS_HST_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_ADDRESS_HST_INFO_TYP IS TABLE OF DM_ADDRESS_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ADDRESS_HST_INFO_tab DM_ADDRESS_HST_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT
    ACCT_NUM  ACCOUNT_NUMBER
    ,'MAILING' ADDR_TYPE
    ,1 ADDR_TYPE_INT_ID
    ,substr(trim(paa.ADDR1),1,40) STREET_1    -- ADDR_1 mapping
    ,trim(paa.ADDR2) STREET_2    -- ADDR_2 mapping
    ,substr(trim(paa.CITY),1,25) CITY
    ,STATE_CODE_ABBR STATE 
    ,ZIP_CODE ZIP_CODE
    ,SUBSTR(ZIP_CODE,7,10) ZIP_PLUS4
    ,COUNTRY_CODE COUNTRY 
    ,nvl2(paa.BAD_ADDR_DATE,'Y','N') NIXIE --If BAD_ADDR_DATE is not null then 'Y' else 'N' 
    ,to_date('02/27/2017', 'MM/DD/YYYY') NIXIE_DATE
    ,'N' NCOA_FLAG
    ,'N' ADDRESS_CLEANSED_FLG
    ,'CSC' ADDRESS_SOURCE
--    ,CREATED_ON CREATED
    ,(select pa.CREATED_ON from PA_ACCT pa
          WHERE pa.ACCT_NUM = paa.ACCT_NUM)  CREATED
    ,'SUNTOLL_CSC_ID' CREATED_BY
    ,to_date('02/27/2017', 'MM/DD/YYYY') LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY
    ,COUNTY_CODE COUNTY_CODE
    ,'SUNTOLL' SOURCE_SYSTEM
--    ,NULL ADDRESS_NUMBER  -- DECODE?
    ,NULL ACTIVITY_ID
    ,0 VERSION
    ,NULL EMP_NUM
    ,NULL STATUS
FROM PA_ACCT_ADDR paa
WHERE DEFAULT_ADDR_FLAG = 'Y'
and paa.acct_num<50000000
--and  paa.ACCT_NUM >= p_begin_acct_num AND   paa.ACCT_NUM <= p_end_acct_num
; -- Source
/*Change FTE_TABLE to the actual table name*/

row_cnt          NUMBER := 0;
v_trac_rec       dm_tracking%ROWTYPE;
v_trac_etl_rec   dm_tracking_etl%ROWTYPE;

BEGIN
  SELECT * INTO v_trac_etl_rec
  FROM  dm_tracking_etl
  WHERE track_etl_id = i_trac_id;
  DBMS_OUTPUT.PUT_LINE('Start '||v_trac_etl_rec.etl_name||' ETL load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  v_trac_etl_rec.status := 'ETL Start ';
  v_trac_etl_rec.proc_start_date := SYSDATE;  
  update_track_proc(v_trac_etl_rec);
 
  SELECT * INTO   v_trac_rec
  FROM   dm_tracking
  WHERE  track_id = v_trac_etl_rec.track_id
  ;

  OPEN C1;   -- (v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ADDRESS_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ADDRESS_HST_INFO_tab.first .. DM_ADDRESS_HST_INFO_tab.last
           INSERT INTO DM_ADDRESS_HST_INFO VALUES DM_ADDRESS_HST_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total ROW_CNT : '||ROW_CNT);

  COMMIT;

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  
  EXCEPTION
  WHEN OTHERS THEN
    v_trac_etl_rec.result_code := SQLCODE;
    v_trac_etl_rec.result_msg := SQLERRM;
    v_trac_etl_rec.proc_end_date := SYSDATE;
    update_track_proc(v_trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS

