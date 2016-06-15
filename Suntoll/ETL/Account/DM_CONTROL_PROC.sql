/********************************************************
*
* Name: DM_CONTROL_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Master process for loading tables
*
*  
********************************************************/

create or replace PROCEDURE DM_CONTROL_PROC 
  (i_source_sys IN dm_control.source_system%TYPE)
IS

  v_source_sys dm_control.source_system%TYPE;
  
  CURSOR c1(p_source_sys dm_control.source_system%TYPE)
  IS 
  SELECT * FROM DM_CONTROL 
  WHERE SOURCE_SYSTEM = p_source_sys
  AND load_order > 0
  ORDER BY load_order
  ;  
  acct_control      dm_control%ROWTYPE;
  trac_rec          dm_tracking%ROWTYPE;
  trac_etl_rec      dm_tracking_etl%ROWTYPE;
  trac_calc_rec     dm_tracking_calc%ROWTYPE;

  sql_string  VARCHAR2(500);
  load_tab    VARCHAR2(50);
  row_cnt     NUMBER := 0;
  p_source_system dm_control.source_system%TYPE := 'SUNTOLL'; 

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start load process at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));  

-- Execute DM_ACCOUNT_INFO first for acct driver.  

  SELECT * INTO acct_control FROM dm_control 
  WHERE source_system = i_source_sys
  AND etl_name = 'DM_ACCOUNT_INFO'
  ;

  START_TRACK_PROC(acct_control, trac_rec);  
  START_ETL_TRACK_PROC(acct_control, trac_rec, trac_etl_rec);    

  PRE_SOURCE_PROC(acct_control, trac_etl_rec);

  DM_ACCOUNT_INFO_PROC(trac_etl_rec);

--    sql_string := 'BEGIN '||acct_control.SOURCE_SYSTEM||'.'||acct_control.etl_name||'_PROC(':||trac_etl_rec||'); END;';
--    DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
--    EXECUTE IMMEDIATE sql_string
--    USING IN OUT trac_etl_rec;
--    DM_ACCOUNT_INFO_PROC(trac_etl_rec); 
--    acct_control.etl_name||'_PROC('||:trac_etl_rec)||'; END;';
    
  POST_DM_PROC(acct_control, trac_etl_rec);
--  VALIDATE_DM_PROC(c_rec, trac_etl_rec);

  FOR c_rec IN c1(i_source_sys)
  LOOP
--    DBMS_OUTPUT.PUT_LINE(c_rec.load_order||' - Start '||c_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

    START_ETL_TRACK_PROC(c_rec, trac_rec, trac_etl_rec);    

    PRE_SOURCE_PROC(c_rec, trac_etl_rec);

--  Dynamic PL/SQL block invokes subprogram:
--  plsql_block := 'BEGIN create_dept(:a, :b, :c, :d); END;';
/*  Specify bind arguments in USING clause.
    Specify mode for first parameter.
    Modes of other parameters are correct by default. */
--  EXECUTE IMMEDIATE plsql_block
--    USING IN OUT new_deptid, new_dname, new_mgrid, new_locid;

--    sql_string := c_rec.etl_name||'_PROC(trac_etl_rec);';    
--    DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
--    EXECUTE IMMEDIATE sql_string;
    
--    sql_string := 'BEGIN '||c_rec.etl_name||'_PROC(trac_etl_rec); END;';
--    DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
--    EXECUTE IMMEDIATE sql_string;

    sql_string := 'BEGIN '||c_rec.etl_name||'_PROC(:id); END;';
    DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);

    EXECUTE IMMEDIATE sql_string
    USING trac_etl_rec.track_etl_id;
    
    POST_DM_PROC(c_rec, trac_etl_rec);

--    VALIDATE_DM_PROC(c_rec, trac_etl_rec);
--    END_ETL_TRACK(trac_etl_rec);

  END LOOP;

  COMMIT;

--  END_ETL_TRACK(trac_etl_rec);
  DBMS_OUTPUT.PUT_LINE('End load process at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
END;
/
SHOW ERRORS



  




