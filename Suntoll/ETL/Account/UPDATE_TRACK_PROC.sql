/********************************************************
*
* Name: UPDATE_TRACK_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Pre load procedure for DM_EMPLOYEE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

declare
--CREATE OR REPLACE PROCEDURE UPDATE_TRACK_PROC (
    track_rec dm_tracking_etl%ROWTYPE);
IS

  ETL_REC   DM_TRACKING_ETL%ROWTYPE;
  CALC_REC   DM_TRACKING_CALC%ROWTYPE;

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  track_rec.PROC_LAST_DATE := SYSDATE;
  UPDATE DM_TRACKING_ETL
    SET ROW = track_rec
  WHERE TRACK_ETL_ID = track_rec.TRACK_ETL_ID
--  AND TRACK_ID = track_rec.TRACK_ID
  ;
  
--  UPDATE DM_TRACKING  WHERE TRACK_ID = track_rec.TRACK_ID ;

  TRACK_REC.DM_NAME := ;
--  START_TRACK(TRACK_REC);

  TOT_REC.TOTAL_TYPE := 'P_ROW_CNT';
  TOT_REC.TOTAL_NAME := SOURCE_TAB;
  
  TOT_REC.TOTAL_TYPE := 'P_ROW_CNT';
  TOT_REC.TOTAL_VALUE := P_ROW_CNT;
  TRACK_TOTAL(TRACK_REC, TOT_REC);

  COMMIT;
  
  
  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
    TOT_REC.RESULT_CODE = SQLCODE;
    TOT_REC.RESULT_MSG = SQLERRM;
    TRACK_ETL(TRACK_REC, TOT_REC);
END;
/
SHOW ERRORS


