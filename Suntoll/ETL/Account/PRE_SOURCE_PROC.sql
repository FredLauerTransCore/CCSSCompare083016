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

--- RETURN calc_rec.calc_value;  
--    execute immediate 'begin :x := ' || 
-- vRunFunctie || '( :p1,  :p2, :vParmOut2 ); end;' using out vParmOut1, vParmIn1, vParmIn2, out vParmOut2;
--      IF v_c_rec.load_drive_field = 'CREATE_ON' THEN
--        sql_string := sql_string||' AND ';
--      ELSIF v_c_rec.load_drive_field = 'OPEN_DATE' THEN

--      v_where := v_where
--              ||v_c_rec.load_drive_field||' > :v_begin_date AND '
--              ||v_c_rec.load_drive_field||' < :v_end_date';
--              ||v_c_rec.load_drive_field||' > to_date(''||v_begin_date||'') AND '
--              ||v_c_rec.load_drive_field||' < to_date(''||v_end_date||'')';

--declare
--    i_calc_rec IN dm_tracking_calc%ROWTYPE,

CREATE OR REPLACE PROCEDURE PRE_SOURCE_PROC (
    i_c_rec       IN      dm_control%ROWTYPE,
    io_trac_rec   IN OUT  dm_tracking_etl%ROWTYPE 
    )
IS

  v_c_rec     dm_control%ROWTYPE;
  v_del_rec   dm_control_detail%ROWTYPE;
--  v_trac_rec  dm_tracking_etl%ROWTYPE;
  calc_rec    dm_tracking_calc%ROWTYPE;
 
  CURSOR c1(p_control_id NUMBER) IS 
  SELECT * FROM DM_CONTROL_DETAIL 
  WHERE control_id = p_control_id
  ORDER BY control_detail_id
;  

  sql_string    VARCHAR2(500); -- := 'truncate table ';
  p_row_cnt     NUMBER := 0;
  v_select      VARCHAR2(100);
  v_into        VARCHAR2(100);
  v_from        VARCHAR2(100);
  v_where       VARCHAR2(500);
  
  v_begin_date    DATE := trunc(SYSDATE-400);
  v_end_date      DATE := trunc(SYSDATE);
  v_begin_acct    pa_acct.acct_num%TYPE;
  v_end_acct      pa_acct.acct_num%TYPE;
  
BEGIN
--  DBMS_OUTPUT.PUT_LINE('Start '||io_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  v_c_rec := i_c_rec;
--  v_etl_rec := io_trac_rec;
--  start_track(track_rec);
--  calc_rec.TRACK_ID :=  v_etl_rec.TRACK_ID;

  v_begin_date := v_c_rec.START_DATE;
  v_end_date := v_c_rec.END_DATE;
  
  io_trac_rec.status := 'PRE process Started';
  update_track_proc(io_trac_rec);

  calc_rec.track_etl_id :=  io_trac_rec.track_etl_id;
  
  FOR idx IN c1(i_c_rec.control_id) 
  LOOP   
    calc_rec.track_calc_id   :=  track_calc_id_seq.NEXTVAL;
  
    calc_rec.calc_func := idx.calc_func;
    calc_rec.calc_field := idx.source_field;
    calc_rec.calc_tab := idx.source_tab;
    calc_rec.calc_value := 0;
    calc_rec.calc_name := 'PRE '||calc_rec.calc_func||' '||calc_rec.calc_field;

    v_select :=   'SELECT '||calc_rec.calc_func||'('||calc_rec.calc_field||')';
    v_into :=     ' INTO :val';
    v_from :=     ' FROM '||calc_rec.calc_tab;
    v_where := ' WHERE 1=1 ';
--    v_where := ' WHERE rownum<11 ';
    IF idx.source_where IS NOT NULL THEN
      v_where :=  v_where||' AND '||idx.source_where;
    END IF;

    IF v_c_rec.load_order = 0 AND idx.CONTROL_DETAIL_ID = 1 THEN 

----  v_query_str := 'begin SELECT COUNT(*) INTO :into_bind FROM emp_' 
----                 || p_loc
----                 || ' WHERE job = :bind_job; end;';
----  EXECUTE IMMEDIATE v_query_str
----    USING out v_num_of_employees, p_job;
--      v_select := v_select|| ', MIN(acct_num), MAX(acct_num) ';
--      v_into := v_into||', :min_acct, :max_acct ';
--      v_where := v_where||' AND ACCT_NUM > 0 AND '
--              ||v_c_rec.load_drive_field||' > :b_dt AND '
--              ||v_c_rec.load_drive_field||' < :e_dt';
--
----      sql_string := v_select||v_into||v_from||v_where;
--      sql_string := v_select||v_from||v_where;
--      
----      DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
----      EXECUTE IMMEDIATE sql_string
----      USING out calc_rec.calc_value, v_begin_acct, v_end_acct, v_begin_date, v_end_date
--      DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
--      EXECUTE IMMEDIATE sql_string
--      INTO calc_rec.calc_value, v_begin_acct, v_end_acct
--      USING v_begin_date, v_end_date
--      ;
  
      select count(acct_num), min(acct_num), max(acct_num)
      INTO calc_rec.calc_value, v_begin_acct, v_end_acct
      from pa_acct
      where CREATED_ON < sysdate
      and CREATED_ON > sysdate-200
      and acct_num>0
      ;
      
      DBMS_OUTPUT.PUT_LINE('Acct num between: '||v_begin_acct||' - '||v_end_acct);
      
      UPDATE DM_TRACKING
      SET   BEGIN_ACCT =  v_begin_acct,
            END_ACCT =  v_end_acct
      WHERE TRACK_ID = io_trac_rec.track_id;
      
    ELSIF v_c_rec.load_drive_field = 'ACCT_NUM' THEN 

      SELECT begin_acct, end_acct
      INTO   v_begin_acct, v_end_acct
      FROM   dm_tracking
      WHERE  track_id = io_trac_rec.track_id
      ;

-- execute immediate 'select dname, loc from dept where deptno = :1'
--   into l_nam, l_loc
--   using l_dept ;    

--      v_select := v_select|| ', MIN(acct_num), MAX(acct_num) ';
--      v_into := v_into||', :min_acct, :max_acct ';
      v_where := v_where||' AND '
              ||v_c_rec.load_drive_field||' > :1 AND '
              ||v_c_rec.load_drive_field||' < :2 ;';

--      sql_string := v_select||v_into||v_from||v_where;
      sql_string := v_select||v_from||v_where;
      
      DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
      EXECUTE IMMEDIATE sql_string
      INTO calc_rec.calc_value
      USING v_begin_acct, v_end_acct
      ;

    ELSE
--      sql_string := v_select||v_into||v_from||v_where;
      sql_string := v_select||v_from||v_where;

      DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
      EXECUTE IMMEDIATE sql_string
      INTO calc_rec.calc_value;
--      EXECUTE IMMEDIATE sql_string
--      USING out calc_rec.calc_value

    END IF;
    DBMS_OUTPUT.PUT_LINE('calc_rec.calc_value : '||calc_rec.calc_value);

    INSERT INTO dm_tracking_calc VALUES calc_rec;      
    COMMIT;

  END LOOP;

  io_trac_rec.status := 'PRE process Completed';
  io_trac_rec.result_code := SQLCODE;
  io_trac_rec.result_msg := SQLERRM;
  update_track_proc(io_trac_rec);
  
  EXCEPTION
  WHEN OTHERS THEN
    io_trac_rec.result_code := SQLCODE;
    io_trac_rec.result_msg := SQLERRM;
    update_track_proc(io_trac_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||io_trac_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||io_trac_rec.result_msg);
END;
/
SHOW ERRORS



      

