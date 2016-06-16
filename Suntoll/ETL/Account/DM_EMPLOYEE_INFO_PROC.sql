/********************************************************
*
* Name: DM_EMPLOYEE_INFO_PROC
* Created by: RH, 5/25/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_EMPLOYEE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

--declare
CREATE OR REPLACE PROCEDURE DM_EMPLOYEE_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_EMPLOYEE_INFO_TYP IS TABLE OF DM_EMPLOYEE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_EMPLOYEE_INFO_tab DM_EMPLOYEE_INFO_TYP;

P_ARRAY_SIZE NUMBER:=1000;

-- * Note (terminated employees are inactive)
CURSOR C1 IS SELECT 
    trim(F_NAME) FIRST_NAME
    ,trim(L_NAME) LAST_NAME
    ,trim(STATUS) ACTIVE_FLAG   --Indicates whether Employee is Active (A) or Inactive (I)
    ,nvl(LEGACY_EMP_CODE,'0') EMP_NUM
    ,nvl(USER_TYPE_CODE,0) JOB_TITLE
    ,trim(nvl(M_INITIAL,'0')) MID_NAME
    ,NULL BIRTH_DT   -- Target is required
--    ,to_date('01-01-1800','MM-DD-YYYY') BIRTH_DT   -- Target is required
    ,nvl(LOCATION_ID,0) STORE_NAME
    ,'SUNTOLL' SOURCE_SYSTEM    
    ,CREATED_ON CREATED
    ,CREATED_BY_USER_ID CREATED_BY
    ,NULL LAST_UPD    -- N/A
    ,NULL LAST_UPD_BY   -- N/A
    ,NULL EMP_STATUS
FROM KS_USER
;   -- Source table SUNTOLL

--sql_string  VARCHAR2(500) := 'delete table ';
row_cnt NUMBER := 0;
v_trac_rec dm_tracking_etl%ROWTYPE;

BEGIN
  select * into v_trac_rec
  from dm_tracking_etl
  where track_etl_id = i_trac_id;
  DBMS_OUTPUT.PUT_LINE('Start '||v_trac_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
--  update_track_proc(io_trac_rec);
  update_track_proc(v_trac_rec);
--  sql_string := sql_string||load_tab;
--  dbms_output.put_line('SQL_STRING : '||sql_string);
--  EXECUTE IMMEDIATE sql_string;
--  COMMIT;

  OPEN c1;  

  v_trac_rec.status := 'Processing';

  LOOP
    
    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_EMPLOYEE_INFO_TAB
    LIMIT p_array_size;

    /*ETL SECTION BEGIN
--    trac_rec.status = 'Processing group'||;

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i IN dm_employee_info_tab.FIRST .. dm_employee_info_tab.LAST
           INSERT INTO dm_employee_info VALUES dm_employee_info_tab(i);

    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_rec);

    EXIT WHEN C1%NOTFOUND;
  END LOOP;

  COMMIT;

  CLOSE c1;

  COMMIT;
--  dbms_output.put_line('END '||load_tab||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
--  dbms_output.put_line('Total ROW count : '||row_cnt);
 -- DBMS_OUTPUT.PUT_LINE('END Count '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

    v_trac_rec.status := 'Completed';
    v_trac_rec.result_code := SQLCODE;
    v_trac_rec.result_msg := SQLERRM;
    update_track_proc(v_trac_rec);

  EXCEPTION
  WHEN OTHERS THEN
    v_trac_rec.result_code := SQLCODE;
    v_trac_rec.result_msg := SQLERRM;
    update_track_proc(v_trac_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_rec.result_msg);
--    RAISE EXCEPTION;
END;
/
SHOW ERRORS


