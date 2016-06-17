/********************************************************
*
* Name: VALIDATION_PROC
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

CREATE OR REPLACE PROCEDURE VALIDATION_PROC (
    i_c_rec     IN      dm_control%ROWTYPE,
    io_trac_rec IN OUT  dm_tracking_etl%ROWTYPE 
    )
IS

  v_trac_rec  dm_tracking%ROWTYPE;
  v_c_rec     dm_control%ROWTYPE;
  v_del_rec   dm_control_detail%ROWTYPE;
  calc_rec    dm_tracking_calc%ROWTYPE;
 
  CURSOR c1(p_control_id NUMBER) IS 
  SELECT * FROM DM_CONTROL_DETAIL 
  WHERE control_id = p_control_id
  ORDER BY control_detail_id
;  

  sql_string    VARCHAR2(500); -- := 'truncate table ';
  p_row_cnt     NUMBER := 0;
  v_select      VARCHAR2(100);
  v_from        VARCHAR2(100);
  v_where       VARCHAR2(500);
  v_begin_date    DATE := trunc(SYSDATE-200);
  v_end_date      DATE := trunc(SYSDATE);
  
BEGIN
  DBMS_OUTPUT.PUT_LINE('Start VALIDATION_PROC for '||i_c_rec.etl_name||' at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  v_c_rec := i_c_rec;

  calc_rec.track_etl_id :=  io_trac_rec.track_etl_id;
  
--  FOR idx IN c1(c_rec.source_system, c_rec.control_id) 
  FOR idx IN c1(i_c_rec.control_id) 
  LOOP
      
    calc_rec.track_calc_id   :=  track_calc_id_seq.NEXTVAL;
  
    calc_rec.calc_func := idx.calc_func;
    calc_rec.calc_field := idx.dm_field;
    calc_rec.calc_tab := i_c_rec.etl_name;
    calc_rec.calc_name := 'DM '||calc_rec.calc_func||' '||calc_rec.calc_field;
    calc_rec.calc_value := 0;
    
    v_select :=   'SELECT '||calc_rec.calc_func||'('||calc_rec.calc_field||')';
    v_from :=     ' FROM '||calc_rec.calc_tab;
    sql_string := v_select||v_from;
    v_where := ' WHERE 1=1 ';

      v_begin_date := v_c_rec.START_DATE;
      v_end_date := v_c_rec.END_DATE;

      sql_string := sql_string||v_where;

      DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
      EXECUTE IMMEDIATE sql_string
      INTO calc_rec.calc_value
      ;
--      USING v_begin_date, v_end_date

      DBMS_OUTPUT.PUT_LINE('calc_rec.calc_value : '||calc_rec.calc_value);

      INSERT INTO dm_tracking_calc VALUES calc_rec;      

      COMMIT;

--    END IF;

--- RETURN calc_rec.calc_value;  
--    execute immediate 'begin :x := ' || 
-- vRunFunctie || '( :p1,  :p2, :vParmOut2 ); end;' using out vParmOut1, vParmIn1, vParmIn2, out vParmOut2;

  END LOOP;

  COMMIT;
  
  
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


