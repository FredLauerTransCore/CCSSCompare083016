/********************************************************
*
* Name: PRE_SOURCE_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Pre load procedure for 
*
********************************************************/

set serveroutput on
set verify on
set echo on

create or replace PROCEDURE              PRE_SOURCE_PROC (
    i_c_rec       IN      dm_control%ROWTYPE,
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
  v_schema      VARCHAR2(100);
  v_select      VARCHAR2(100);
  v_into        VARCHAR2(100);
  v_from        VARCHAR2(100);
  v_where       VARCHAR2(500);

BEGIN
  v_c_rec := i_c_rec;
  calc_rec.calc_type := 'PRE';
  --v_schema := v_c_rec.SOURCE_SYSTEM||'.';
  SELECT * INTO v_etl_rec
  FROM  dm_tracking_etl
  WHERE track_etl_id = i_trac_id;

--  calc_rec.TRACK_ID :=  v_etl_rec.TRACK_ID;
--  DBMS_OUTPUT.PUT_LINE('Start PRE_SOURCE_PROC for '||v_etl_rec.etl_name||' at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  SELECT * INTO v_trac_rec
  FROM  dm_tracking
  WHERE track_id = v_etl_rec.track_id
  ;
--  DBMS_OUTPUT.PUT_LINE('Acct between: '||v_trac_rec.begin_acct||' - '||v_trac_rec.end_acct);

  v_etl_rec.status := calc_rec.calc_type||' process Started';
  update_track_proc(v_etl_rec);

  calc_rec.track_etl_id :=  v_etl_rec.track_etl_id;
  FOR idx IN c1(v_c_rec.control_id)
  LOOP
    calc_rec.track_calc_id   :=  track_calc_id_seq.NEXTVAL;
    calc_rec.control_calc_id := idx.control_calc_id;
    calc_rec.calc_func := idx.calc_func;
    calc_rec.calc_field := idx.source_field;
    calc_rec.calc_tab := idx.source_tab;
    calc_rec.calc_value := 0;
    calc_rec.calc_name := calc_rec.calc_type||' '||calc_rec.calc_func||' '||calc_rec.calc_field;

    v_select :=   'SELECT '||calc_rec.calc_func||'('||calc_rec.calc_field||')';
--    v_into :=     ' INTO :val';
    v_from :=     ' FROM '||v_schema||calc_rec.calc_tab;
    v_where := ' WHERE 1=1 ';  -- rownum < 1001
    IF idx.source_where IS NOT NULL THEN
      v_where :=  v_where||' AND '||idx.source_where;
    END IF;

    if v_c_rec.source_acct_field is not null then
      v_where := v_where||' and '||v_c_rec.source_acct_field||' >= '||v_trac_rec.begin_acct
                        ||' and '||v_c_rec.source_acct_field||' <= '||v_trac_rec.end_acct;
    end if;

-- execute immediate 'select dname, loc from dept where deptno = :1'
--   into l_nam, l_loc using l_dept ;
--              ||v_c_rec.load_drive_field||' >= :1 AND '
--              ||v_c_rec.load_drive_field||' <= :2 ;';
--      v_where := v_where||' AND ACCT_NUM >= :1 AND ACCT_NUM <= :2 ;';

    sql_string := v_select||v_from||v_where;  -- v_select||v_into||v_from||v_where;
    
    DBMS_OUTPUT.PUT_LINE(calc_rec.calc_type||' sql : '||sql_string);
    calc_rec.sql_text := sql_string;
    EXECUTE IMMEDIATE sql_string
    INTO calc_rec.calc_value;
--      USING out calc_rec.calc_value
--      USING v_begin_acct, v_end_acct

    DBMS_OUTPUT.PUT_LINE('Total '||calc_rec.calc_type||' '||calc_rec.calc_func||' : '||calc_rec.calc_value);

    INSERT INTO dm_tracking_calc VALUES calc_rec;
    COMMIT;
    calc_rec.result_code := SQLCODE;
    calc_rec.result_msg := SQLERRM;
    update_track_calc_proc(calc_rec);

  END LOOP;

  v_etl_rec.status := calc_rec.calc_type||' process Completed';
  --  update_track_proc(v_etl_rec);
  
  EXCEPTION
  WHEN OTHERS THEN
--    v_etl_rec.result_code := SQLCODE;
--    v_etl_rec.result_msg := SQLERRM;
    calc_rec.result_code := SQLCODE;
    calc_rec.result_msg := SQLERRM;
    update_track_calc_proc(calc_rec);
     DBMS_OUTPUT.PUT_LINE('PRE ERROR CODE: '||calc_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||calc_rec.result_msg);
END;
/
SHOW ERRORS

commit;

      

