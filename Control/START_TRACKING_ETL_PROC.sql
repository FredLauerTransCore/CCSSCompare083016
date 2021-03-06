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
CREATE OR REPLACE PROCEDURE START_ETL_TRACK_PROC (
    i_con_rec   IN  dm_control%ROWTYPE, 
    i_trac_id   IN  dm_tracking.track_id%TYPE, 
    o_etl_rec   OUT dm_tracking_etl%ROWTYPE)
IS

  trac_rec    dm_tracking%ROWTYPE;
  etl_rec     dm_tracking_etl%ROWTYPE;
--  calc_rec    dm_tracking_calc%ROWTYPE;

BEGIN
--  DBMS_OUTPUT.PUT_LINE('Start '||i_con_rec.etl_name||' ETL track at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  etl_rec.track_etl_id := track_etl_id_seq.NEXTVAL;
  etl_rec.track_id := i_trac_id;
  etl_rec.etl_name := i_con_rec.etl_name;
  etl_rec.proc_start_date := SYSDATE;
  etl_rec.status := 'Start';
  etl_rec.dm_load_cnt := 0;

  INSERT INTO DM_TRACKING_ETL VALUES etl_rec;
  COMMIT;
  
  o_etl_rec :=  etl_rec;
  
  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('TRACK ETL ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
    etl_rec.RESULT_CODE := SQLCODE;
    etl_rec.RESULT_MSG := SQLERRM;
    update_track_proc(etl_rec);
END;
/
SHOW ERRORS


