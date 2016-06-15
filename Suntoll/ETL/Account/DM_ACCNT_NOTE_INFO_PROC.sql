/********************************************************
*
* Name: DM_ACCNT_NOTE_INFO_PROC
* Created by: RH, 5/17/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCNT_NOTE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

--DECLARE
CREATE OR REPLACE PROCEDURE DM_ACCNT_NOTE_INFO_PROC
--  (io_trac_rec IN OUT dm_tracking_etl%ROWTYPE)
  (io_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_ACCNT_NOTE_INFO_TYP IS TABLE OF DM_ACCNT_NOTE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCNT_NOTE_INFO_tab DM_ACCNT_NOTE_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1(p_begin_acct_num  pa_acct.acct_num%TYPE, 
          p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,PROB_CODE NOTE_TYPE
    ,TYPE_CODE NOTE_SUB_TYPE
    ,NOTE NOTE
    ,CREATED NOTE_CREATED_DT
    ,CREATED CREATED
    ,EMP_CODE CREATED_BY
    ,CREATED LAST_UPD
    ,EMP_CODE LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
FROM NOTE
WHERE ACCT_NUM >= p_begin_acct_num
AND   ACCT_NUM <= p_end_acct_num
--and rownum<11
;   -- Source

--sql_string  VARCHAR2(500) := 'truncate table ';

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
--  sql_string := sql_string||load_tab;
--  dbms_output.put_line('SQL_STRING : '||sql_string);
--  EXECUTE IMMEDIATE sql_string;
--  COMMIT;
 
  SELECT begin_acct, end_acct
  INTO   v_begin_acct, v_end_acct
  FROM   dm_tracking
  WHERE  track_id = v_trac_rec.track_id
  ;
  
  OPEN C1(v_begin_acct,v_end_acct);  
  v_trac_rec.status := 'Processing';

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCNT_NOTE_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ACCNT_NOTE_INFO_tab.first .. DM_ACCNT_NOTE_INFO_tab.last
           INSERT INTO DM_ACCNT_NOTE_INFO VALUES DM_ACCNT_NOTE_INFO_tab(i);

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

