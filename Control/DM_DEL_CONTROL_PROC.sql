/********************************************************
*
* Name: DM_DEL_CONTROL_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Delete DM tables
*
*  
********************************************************/
set serveroutput on
set verify on
set echo on

create or replace PROCEDURE DM_DEL_CONTROL_PROC 
  (i_track_id IN dm_tracking.track_id%TYPE)
IS

  CURSOR c1(p_source_sys dm_control.source_system%TYPE)
  IS 
  SELECT * FROM DM_CONTROL 
  WHERE UPPER(source_system) = p_source_sys
  AND disabled IS NULL
--  AND load_order > 0
  ORDER BY load_order
  ;  
  c_acct            dm_control%ROWTYPE;
  trac_rec          dm_tracking%ROWTYPE;
  trac_etl_rec      dm_tracking_etl%ROWTYPE;
  trac_calc_rec     dm_tracking_calc%ROWTYPE;

  load_tab    VARCHAR2(50);
  row_cnt     NUMBER := 0;
--  v_source_sys dm_control.source_system%TYPE := 'SUNTOLL'; 

  sql_string    VARCHAR2(500) := 'delete ';
  p_row_cnt     NUMBER := 0;
  v_select      VARCHAR2(100);
  v_from        VARCHAR2(100);
  v_where       VARCHAR2(500);
  v_schema      VARCHAR2(100);

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start ldelete process at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));  
  
  select * into trac_rec
  from DM_TRACKING
  where TRACK_ID = i_track_id;  

  FOR c_rec IN c1(trac_rec.SOURCE_SYSTEM)
  LOOP
    DBMS_OUTPUT.PUT_LINE(c_rec.load_order||' - Start '||c_rec.etl_name||' delete at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

    sql_string := 'delete ';
    sql_string := sql_string||v_schema||c_rec.etl_name;
--    v_where := ' WHERE 1=1 ';
    v_where := ' WHERE source_system = '''||c_rec.source_system||'''';

    sql_string := sql_string||v_where;

--    sql_string := 'BEGIN '||trac_rec.SOURCE_SYSTEM||'_user.'||c_rec.etl_name||'_PROC(:id); END;';
    DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
    EXECUTE IMMEDIATE sql_string;
    
--    SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('deleted '||c_rec.etl_name||' : '||SQL%ROWCOUNT);

  END LOOP;

  COMMIT;

--  END_TRACK(trac_rec);
  DBMS_OUTPUT.PUT_LINE('End delete process at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  COMMIT;

  EXCEPTION
  WHEN OTHERS THEN
    trac_etl_rec.result_code := SQLCODE;
    trac_etl_rec.result_msg := SQLERRM;
    update_track_proc(trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||trac_etl_rec.result_msg);
END;
/
SHOW ERRORS



  




