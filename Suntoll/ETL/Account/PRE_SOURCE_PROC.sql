/********************************************************
*
* Name: PRE_SOURCE_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: Pre load procedure for DM_EMPLOYEE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

declare
--
CREATE OR REPLACE PROCEDURE PRE_SOURCE_PROC (c_rec  IN dm_control%ROWTYPE)
IS

  Trac_Rec Dm_Tracking%ROWTYPE;
  tot_rec   dm_tracking_totals%ROWTYPE;
  ent_rec   dm_tracking_entity%ROWTYPE;
--TYPE DM_TRACKING_TYP IS TABLE OF DM_TRACKING%ROWTYPE INDEX BY BINARY_INTEGER;
--DM_EMPLOYEE_INFO_tab DM_EMPLOYEE_INFO_TYP;
--P_ARRAY_SIZE NUMBER:=1000;

  sql_string    VARCHAR2(500) := 'truncate table ';
  p_row_cnt     NUMBER := 0;
  v_select    VARCHAR2(100);
  v_into    VARCHAR2(100);
  v_from    VARCHAR2(100);
  v_where    VARCHAR2(500);

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  TRACK_REC.DM_NAME := DM_LOAD_TAB;
  START_TRACK(TRACK_REC);

  TOT_REC.TOTAL_TYPE := 'P_ROW_CNT';
  TOT_REC.TOTAL_NAME := SOURCE_TAB;

    V_SELECT := GET_SELECT_CLAUSE(c_rec);
    V_INTO := GET_INTO_CLAUSE(c_rec);
    V_FROM := GET_FROM_CLAUSE(c_rec);
    V_WHERE := GET_WHERE_CLAUSE(c_rec);
    sql_string := 
        'SELECT '||V_SELECT
        ' INTO '||V_INTO
        ' FROM '||V_FROM
        ' WHERE '||V_WHERE;
    DBMS_OUTPUT.PUT_LINE('sql_string : '||sql_string);
    EXECUTE IMMEDIATE sql_string;
    execute immediate 'begin :x := ' || vRunFunctie || '( :p1,  :p2, :vParmOut2 ); end;' using out vParmOut1, vParmIn1, vParmIn2, out vParmOut2;

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
    TRACK_ENTITY(TRACK_REC, TOT_REC);
END;
/
SHOW ERRORS


