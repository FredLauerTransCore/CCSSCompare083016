/********************************************************
*
* Name: DM_TRACK_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Pre load procedure for DM_EMPLOYEE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

declare
--CREATE OR REPLACE PROCEDURE DM_TRACK_PROC (
    IN/OUT trac_rec dm_tracking%ROWTYPE);
IS

  ENT_REC   DM_TRACKING_ENTITY%ROWTYPE;
  TOT_REC   DM_TRACKING_TOTALS%ROWTYPE;
--TYPE DM_TRACKING_TYP IS TABLE OF DM_TRACKING%ROWTYPE INDEX BY BINARY_INTEGER;
--DM_EMPLOYEE_INFO_tab DM_EMPLOYEE_INFO_TYP;
--P_ARRAY_SIZE NUMBER:=1000;

SQL_STRING    varchar2(500) := 'truncate table ';
DM_LOAD_TAB    varchar2(50) := 'DM_EMPLOYEE_INFO';
SOURCE_TAB    varchar2(50) := 'KS_USER';
P_ROW_CNT     NUMBER := 0;
FROM_CLAUSE    varchar2(100) := 'KS_USER';
WHERE_CLAUSE    varchar2(500);

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  TRACK_REC.DM_NAME := DM_LOAD_TAB;
  START_TRACK(TRACK_REC);

  TOT_REC.TOTAL_TYPE := 'P_ROW_CNT';
  TOT_REC.TOTAL_NAME := SOURCE_TAB;
  
    /*Count Enity */
  select count(1) into P_ROW_CNT
  FROM KS_USER  -- source table
  ;
  
  TOT_REC.TOTAL_TYPE := 'P_ROW_CNT';
  TOT_REC.TOTAL_VALUE := P_ROW_CNT;
  TRACK_TOTAL(TRACK_REC, TOT_REC);

  COMMIT;
  
  
  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
    trac_rec.RESULT_CODE = SQLCODE;
    trac_rec.RESULT_MSG = SQLERRM;
    dm_track_proc(trac_rec);
END;
/
SHOW ERRORS


