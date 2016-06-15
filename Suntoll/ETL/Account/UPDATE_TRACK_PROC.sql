/********************************************************
*
* Name: UPDATE_TRACK_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Update tracking record with latest data
*
********************************************************/

set serveroutput on
set verify on
set echo on

--declare
CREATE OR REPLACE PROCEDURE UPDATE_TRACK_PROC (
    io_trac_rec IN OUT dm_tracking_etl%ROWTYPE)
IS

  v_etl_rec   dm_tracking_etl%ROWTYPE;
  v_calc_rec   dm_tracking_calc%ROWTYPE;

BEGIN
--  DBMS_OUTPUT.PUT_LINE('Start '||io_trac_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  v_etl_rec := io_trac_rec;
  v_etl_rec.proc_last_date := SYSDATE;
  UPDATE DM_TRACKING_ETL
    SET ROW = v_etl_rec
  WHERE track_etl_id = v_etl_rec.track_etl_id
--  AND TRACK_ID = track_rec.TRACK_ID
  ;

  COMMIT;
  
  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
--    TOT_REC.RESULT_CODE = SQLCODE;
--    TOT_REC.RESULT_MSG = SQLERRM;
--    TRACK_ETL(TRACK_REC, TOT_REC);
END;
/
SHOW ERRORS


