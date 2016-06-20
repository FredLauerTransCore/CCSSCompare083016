/********************************************************
*
* Name: UPDATE_TRACK_CALC_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Update tracking record with latest data
*
********************************************************/

set serveroutput on
set verify on
set echo on

--declare
CREATE OR REPLACE PROCEDURE UPDATE_TRACK_CALC_PROC (
    io_trac_rec IN OUT dm_tracking_calc%ROWTYPE)
IS

  v_etl_rec   dm_tracking_etl%ROWTYPE;
  v_calc_rec   dm_tracking_calc%ROWTYPE;

BEGIN  
  v_calc_rec := io_trac_rec;
--  v_calc_rec.proc_last_date := SYSDATE;
  UPDATE DM_TRACKING_CALC
    SET ROW = v_calc_rec
  WHERE track_calc_id = v_calc_rec.track_calc_id
  ;
--  UPDATE DM_TRACKING
--  set PROC_END_DATE = SYSDATE,
--      STATUS = track_calc_id.etl_name||' '||track_calc_id.status
--  where TRACK_ID = v_etl_rec.track_id;
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


