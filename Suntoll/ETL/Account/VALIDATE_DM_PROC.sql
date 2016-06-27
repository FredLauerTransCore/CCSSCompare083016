/********************************************************
*
* Name: VALIDATE_DM_PROC
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

CREATE OR REPLACE PROCEDURE VALIDATE_DM_PROC (
    i_c_rec       IN      dm_control%ROWTYPE,
--    io_trac_rec IN OUT  dm_tracking_etl%ROWTYPE 
    i_trac_id   dm_tracking_etl.track_etl_id%TYPE
    )
IS

  v_c_rec     dm_control%ROWTYPE;
  v_del_rec   dm_control_detail%ROWTYPE;
  v_trac_rec  dm_tracking%ROWTYPE;
  v_etl_rec   dm_tracking_etl%ROWTYPE;
  pre_calc_rec      dm_tracking_calc%ROWTYPE;
  post_calc_rec     dm_tracking_calc%ROWTYPE;
 
  CURSOR c1(p_control_id NUMBER) IS 
  SELECT * FROM DM_CONTROL_DETAIL 
  WHERE control_id = p_control_id
  AND disabled IS NULL
  ORDER BY control_calc_id
;  
  
BEGIN
  DBMS_OUTPUT.PUT_LINE(i_c_rec.control_id||' - Start VALIDATION_PROC for '||i_c_rec.etl_name||' at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  v_c_rec := i_c_rec;
  SELECT * INTO v_etl_rec
  FROM  dm_tracking_etl
  WHERE track_etl_id = i_trac_id;

  v_etl_rec.status := 'Validation process Started';
  update_track_proc(v_etl_rec);

--  FOR idx IN c1(c_rec.source_system, c_rec.control_id) 
  FOR idx IN c1(i_c_rec.control_id) 
  LOOP
      
    select * into pre_calc_rec
    from dm_tracking_calc dtc
    where dtc.calc_type = 'PRE'
    and   dtc.TRACK_ETL_ID = v_etl_rec.track_etl_id
    ;

    select * into post_calc_rec
    from dm_tracking_calc dtc
    where dtc.calc_type = 'POST'
    and   dtc.TRACK_ETL_ID = v_etl_rec.track_etl_id
    ;
 
DBMS_OUTPUT.PUT_LINE('PRE value: '||pre_calc_rec.CALC_VALUE||' and POST value: '||post_calc_rec.CALC_VALUE||
          ' for '||idx.calc_func||' of '||i_c_rec.etl_name||'.'||idx.target_field||'; '||v_etl_rec.result_msg);
   
    if nvl(pre_calc_rec.CALC_VALUE,0) != nvl(post_calc_rec.CALC_VALUE,0) then 
      v_etl_rec.result_code := nvl(v_etl_rec.result_code,0)-1;  
      v_etl_rec.result_msg := 'ERROR: PRE value: '||pre_calc_rec.CALC_VALUE||' and POST value: '||post_calc_rec.CALC_VALUE||
          ' does not match for '||idx.calc_func||' of '||i_c_rec.etl_name||'.'||idx.target_field||'; '||v_etl_rec.result_msg;

      DBMS_OUTPUT.PUT_LINE('result_msg : '||v_etl_rec.result_msg);
      
    END IF;
    update_track_proc(v_etl_rec);
--    calc_rec.calc_func := idx.calc_func;
--    calc_rec.calc_field := idx.target_field;
--    calc_rec.calc_tab := i_c_rec.etl_name;

  END LOOP;

  COMMIT;

  v_etl_rec.status := 'VALID -';
  if  nvl(v_etl_rec.result_code,0) != 0 then
    v_etl_rec.status := 'NOT '||v_etl_rec.status;
  end if;  
  v_etl_rec.status := v_etl_rec.status||' LOAD process Completed';
  
  v_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_etl_rec);
  UPDATE DM_TRACKING
  set result_code = v_etl_rec.result_code,
      result_msg = v_etl_rec.result_msg
  where TRACK_ID = v_etl_rec.track_id;
   
  COMMIT;
    
--  io_trac_rec := v_etl_rec;
  DBMS_OUTPUT.PUT_LINE('----------- Done ------------');  
  
  EXCEPTION
  WHEN OTHERS THEN
    v_etl_rec.result_code := SQLCODE;
    v_etl_rec.result_msg := SQLERRM;
    update_track_proc(v_etl_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_etl_rec.result_msg);
END;
/
SHOW ERRORS


