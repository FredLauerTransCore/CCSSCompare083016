/********************************************************
*
* Name: DM_CONTROL_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Master process for loading tables
*
*  
********************************************************/
set serveroutput on
set verify on
set echo on

create or replace PROCEDURE DM_CONTROL_PROC 
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

  sql_string  VARCHAR2(500);
  load_tab    VARCHAR2(50);
  row_cnt     NUMBER := 0;
  v_source_sys dm_control.source_system%TYPE := 'SUNTOLL'; 

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start load process at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));  

  UPDATE DM_TRACKING
  set STATUS = 'Start load process',
      PROC_START_DATE = SYSDATE
  where TRACK_ID = i_track_id;
  commit;
  
  select * into trac_rec
  from DM_TRACKING
  where TRACK_ID = i_track_id;  

  FOR c_rec IN c1(trac_rec.SOURCE_SYSTEM)
  LOOP
    DBMS_OUTPUT.PUT_LINE(c_rec.load_order||' - Start '||c_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
--    DBMS_OUTPUT.PUT_LINE(c_rec.load_order||' - Start '||c_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

    START_ETL_TRACK_PROC(c_rec, i_track_id, trac_etl_rec);    

    PRE_SOURCE_PROC(c_rec, trac_etl_rec.track_etl_id);

--  Dynamic PL/SQL block invokes subprogram:
--  plsql_block := 'BEGIN create_dept(:a, :b, :c, :d); END;';
/*  Specify bind arguments in USING clause.
    Specify mode for first parameter.
    Modes of other parameters are correct by default. */
--  EXECUTE IMMEDIATE plsql_block
--    USING IN OUT new_deptid, new_dname, new_mgrid, new_locid;

    sql_string := 'BEGIN '||trac_rec.SOURCE_SYSTEM||'_user.'||c_rec.etl_name||'_PROC(:id); END;';
--    DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);

    EXECUTE IMMEDIATE sql_string
    USING trac_etl_rec.track_etl_id;
    
    POST_DM_PROC(c_rec, trac_etl_rec.track_etl_id);

    VALIDATE_DM_PROC(c_rec, trac_etl_rec.track_etl_id);
--    END_ETL_TRACK(trac_etl_rec);

  END LOOP;

  COMMIT;

--  END_TRACK(trac_rec);
  DBMS_OUTPUT.PUT_LINE('End load process at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  UPDATE DM_TRACKING
  set PROC_END_DATE = SYSDATE
      ,Status = 'Completed process'
      ,result_code = trac_etl_rec.result_code
      ,result_msg = trac_etl_rec.result_msg
  where TRACK_ID = i_track_id;
  COMMIT;

  EXCEPTION
  WHEN OTHERS THEN
    trac_etl_rec.result_code := SQLCODE;
    trac_etl_rec.result_msg := SQLERRM;
    update_track_proc(trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||trac_etl_rec.result_code);
END;
/
SHOW ERRORS



  




