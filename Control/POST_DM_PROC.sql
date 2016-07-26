/********************************************************
*
* Name: POST_DM_PROC
* Created by: RH, 6/14/2016
* Revision: 1.0
* Description: Post load procedure for DM table for
*             calculations of Counts and summs
*
********************************************************/

set serveroutput on
set verify on
set echo on

--declare
--    i_calc_rec IN dm_tracking_calc%ROWTYPE,

create or replace PROCEDURE              POST_DM_PROC (
    i_c_rec     dm_control%ROWTYPE,
    i_trac_id   dm_tracking_etl.track_etl_id%TYPE
    ) authid current_user
IS

  v_c_rec     dm_control%ROWTYPE;
  v_del_rec   dm_control_detail%ROWTYPE;
  v_trac_rec  dm_tracking%ROWTYPE;
  v_etl_rec   dm_tracking_etl%ROWTYPE;
  calc_rec    dm_tracking_calc%ROWTYPE;


  CURSOR c1(p_control_id NUMBER) IS
  SELECT * FROM DM_CONTROL_DETAIL
  WHERE control_id = p_control_id
  AND disabled IS NULL
  ORDER BY control_calc_id
;

  sql_string    VARCHAR2(500); -- := 'truncate table ';
  p_row_cnt     NUMBER := 0;
  v_select      VARCHAR2(100);
  v_from        VARCHAR2(100);
  v_where       VARCHAR2(500);
  v_schema      VARCHAR2(100);

BEGIN
  v_c_rec := i_c_rec;
  calc_rec.calc_type := 'POST';
--  v_schema  := 'DMSTAGING_DEV.';

  SELECT * INTO v_etl_rec
  FROM  dm_tracking_etl
  WHERE track_etl_id = i_trac_id;

--  DBMS_OUTPUT.PUT_LINE('Start POST_DM_PROC for '||v_etl_rec.etl_name||' at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  v_etl_rec.proc_start_date := SYSDATE;
  v_etl_rec.status := calc_rec.calc_type||' process Started';
  update_track_proc(v_etl_rec);

  SELECT * INTO v_trac_rec
  FROM  dm_tracking
  WHERE track_id = v_etl_rec.track_id
  ;
  calc_rec.track_etl_id :=  v_etl_rec.track_etl_id;

--  FOR idx IN c1(c_rec.source_system, c_rec.control_id)
  FOR idx IN c1(i_c_rec.control_id)
  LOOP

    calc_rec.track_calc_id   := track_calc_id_seq.NEXTVAL;
    calc_rec.control_calc_id := idx.control_calc_id;

    calc_rec.calc_func := idx.calc_func;
    calc_rec.calc_field := idx.target_field;
    calc_rec.calc_tab := i_c_rec.etl_name;
    calc_rec.calc_name := calc_rec.calc_type||'  '||calc_rec.calc_func||' '||calc_rec.calc_field;
    calc_rec.calc_value := 0;

    v_select :=   'SELECT '||calc_rec.calc_func||'('||calc_rec.calc_field||')';
    v_from :=     ' FROM '||v_schema||calc_rec.calc_tab;
    sql_string := v_select||v_from;
--    v_where := ' WHERE 1=1 ';
    v_where := ' WHERE source_system = '''||v_c_rec.source_system||'''';

    if v_c_rec.target_acct_field is not null then
      v_where := v_where||' and '||v_c_rec.target_acct_field||' >= '||v_trac_rec.begin_acct
                        ||' and '||v_c_rec.target_acct_field||' <= '||v_trac_rec.end_acct;
    end if;

    IF idx.target_where IS NOT NULL THEN
      v_where :=  v_where||' AND '||idx.target_where;
    END IF;

    sql_string := sql_string||v_where;

    DBMS_OUTPUT.PUT_LINE(calc_rec.calc_type||' sql : '||sql_string);
    calc_rec.sql_text := sql_string;
    EXECUTE IMMEDIATE sql_string
    INTO calc_rec.calc_value
    ;
--      USING v_begin_date, v_end_date

    DBMS_OUTPUT.PUT_LINE('Total '||calc_rec.calc_type||' '||calc_rec.calc_func||' : '||calc_rec.calc_value);

    INSERT INTO dm_tracking_calc VALUES calc_rec;

    COMMIT;

    calc_rec.result_code := SQLCODE;
    calc_rec.result_msg := SQLERRM;
    update_track_calc_proc(calc_rec);

  END LOOP;

  COMMIT;

  v_etl_rec.status := calc_rec.calc_type||' process Completed';
--  update_track_proc(v_etl_rec);
--  update_track_calc_proc(calc_rec);

  EXCEPTION
  WHEN OTHERS THEN
--    v_etl_rec.result_code := SQLCODE;
--    v_etl_rec.result_msg := SQLERRM;
    calc_rec.result_code := SQLCODE;
    calc_rec.result_msg := SQLERRM;
    update_track_calc_proc(calc_rec);
     DBMS_OUTPUT.PUT_LINE('Post ERROR CODE: '||calc_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||calc_rec.result_msg);
END;
/
SHOW ERRORS

commit;