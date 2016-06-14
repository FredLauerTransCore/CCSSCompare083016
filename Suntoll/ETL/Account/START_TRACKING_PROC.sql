/********************************************************
*
* Name: START_TRACK_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Pre load procedure for DM_EMPLOYEE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

--declare
CREATE OR REPLACE PROCEDURE START_TRACK_PROC (
    i_rec IN  dm_control%ROWTYPE,
    o_rec OUT dm_tracking%ROWTYPE)
IS

  etl_rec     dm_tracking_etl%ROWTYPE;
  calc_rec    dm_tracking_calc%ROWTYPE;
  trac_rec    dm_tracking%ROWTYPE;

BEGIN
--  DBMS_OUTPUT.PUT_LINE('Start '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  trac_rec.track_id := track_id_seq.NEXTVAL;
--  trac_rec.dm_name := i_rec.dm_name;
--  trac_rec.PROC_START_DATE := SYSDATE;
  trac_rec.source_system := i_rec.source_system;
  

  INSERT INTO DM_TRACKING VALUES trac_rec;
  COMMIT;
  
  o_rec :=  trac_rec;
  
  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
    trac_rec.RESULT_CODE := SQLCODE;
    trac_rec.RESULT_MSG := SQLERRM;
 --   track_proc(trac_rec);
END;
/
SHOW ERRORS


