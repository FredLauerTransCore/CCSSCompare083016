/********************************************************
*
* Name: DM_CONTROL_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Master process for loading tables
*
*  
********************************************************/


declare
--CREATE OR REPLACE PROCEDURE DM_CONTROL_PROC (p_source_system dm_control%TYPE)
--IS

  p_source_system dm_control.source_system%TYPE := 'SUNTOLL'
  
  CURSOR c1 IS 
  SELECT * FROM DM_CONTROL 
  WHERE SOURCE_SYSTEM = 'SUNTOLL'
  ORDER BY load_order
  ;  
  acct_control      dm_control%ROWTYPE;
  trac_rec          dm_tracking%ROWTYPE;
  trac_etl_rec      dm_tracking_etl%ROWTYPE;

  sql_string  VARCHAR2(500);
  load_tab    VARCHAR2(50);
  row_cnt     NUMBER := 0;

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start load process at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

-- Execute Acct_info first for acct driver.

  START_TRACK(trac_rec);  
  
  SELECT * FROM DM_CONTROL 
  INTO  acct_control
  WHERE SOURCE_SYSTEM = p_source_system
  AND ETL_NAME = 'DM_ACCNT_INFO'
  ;

  START_ETL_TRACK(trac_rec, acct_control, trac_etl_rec);    
  
  PRE_SOURCE_PROC(acct_control, trac_etl_rec);
  DM_ACCT_INFO_PROC(acct_control);
  POST_DM_PROC(acct_control);
  
  FOR c_rec IN c1
  LOOP
    DBMS_OUTPUT.PUT_LINE(c_rec.load_order||' - Start '||c_rec.dm_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

    START_ETL_TRACK(trac_rec, c_rec, trac_etl_rec)   

    PRE_SOURCE_PROC(c_rec, trac_etl_rec);
    
    sql_string := c_rec.dm_name||'_PROC(trac_etl_rec)';
    DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
    EXECUTE IMMEDIATE sql_string;
--    END_TRACK(track_rec);

    POST_DM_PROC(c_rec, trac_etl_rec);

  END LOOP;

  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Start load process at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
END;
/
SHOW ERRORS


