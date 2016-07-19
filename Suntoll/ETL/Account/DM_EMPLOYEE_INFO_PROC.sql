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

-- 6/20/2016 RH Added Tracking process and parameters

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
    ,trim(nvl(M_INITIAL,'UNDEFINED')) MID_NAME
--    ,NULL BIRTH_DT   -- Target is required
    ,to_date('11/17/1858','MM/DD/YYYY') BIRTH_DT   -- Target is required
    ,nvl(LOCATION_ID,0) STORE_NAME
    ,'SUNTOLL' SOURCE_SYSTEM    
    ,CREATED_ON CREATED
    ,CREATED_BY_USER_ID CREATED_BY
    ,NULL LAST_UPD    -- N/A
    ,NULL LAST_UPD_BY   -- N/A
--    ,NULL EMP_STATUS
FROM KS_USER
where LEGACY_EMP_CODE is not null
;   -- Source table SUNTOLL

row_cnt          NUMBER := 0;
v_trac_rec       dm_tracking%ROWTYPE;
v_trac_etl_rec   dm_tracking_etl%ROWTYPE;

BEGIN
  SELECT * INTO v_trac_etl_rec
  FROM  dm_tracking_etl
  WHERE track_etl_id = i_trac_id;
  DBMS_OUTPUT.PUT_LINE('Start '||v_trac_etl_rec.etl_name||' ETL load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  v_trac_etl_rec.status := 'ETL Start ';
  v_trac_etl_rec.proc_start_date := SYSDATE;  
  update_track_proc(v_trac_etl_rec);
 
  SELECT * INTO   v_trac_rec
  FROM   dm_tracking
  WHERE  track_id = v_trac_etl_rec.track_id
  ;

  OPEN C1; 
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

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
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Total load count : '||ROW_CNT);

  COMMIT;

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  EXCEPTION
  WHEN OTHERS THEN
    v_trac_etl_rec.result_code := SQLCODE;
    v_trac_etl_rec.result_msg := SQLERRM;
    v_trac_etl_rec.proc_end_date := SYSDATE;
    update_track_proc(v_trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS

