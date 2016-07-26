/********************************************************
*
* Name: DM_PA_PLAZA_PROC
* Created by: RH, 6/5/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PA_PLAZA
*
********************************************************/

set serveroutput on
set verify on
set echo on

--declare
CREATE OR REPLACE PROCEDURE DM_PA_PLAZA_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_PA_PLAZA_TYP IS TABLE OF DM_PA_PLAZA%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PA_PLAZA_tab DM_PA_PLAZA_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1 IS SELECT 
    ADMINTYPE_ADMIN_TYPE_CODE
    ,AUTHCODE_AUTHORITY_CODE
    ,AVC_THRESHOLD
    ,CLASS2_RATE_FLAG
    ,COUNTY_COUNTY_CODE
    ,CROSS_READ_PLAZA_ID
    ,FACCODE_FACILITY_CODE
    ,FTE_PLAZA
    ,GEOG_COORD_LAT
    ,GEOG_COORD_LONG
    ,IAG_PLAZA_ID
    ,IN_LANE_DISC_PERCENT
    ,I_TOLL_PARTICIPATION
    ,MILE_POST
    ,MIN_FARE_PLAZA_ID
    ,N_BOUND_TIME
    ,PLAZA_ID
    ,trim(PLAZA_NAME) PLAZA_NAME
    ,trim(PLAZA_SHORT_NAME) PLAZA_SHORT_NAME
    ,trim(PLAZA_SLAVE_NUM)  PLAZA_SLAVE_NUM
--  ,P_TOLL_PARTICIPATION   --  Only in Suntoll
    ,RAMPTYPE_RAMP_TYPE_CODE
    ,'NO' REBATE_REBATE_PROGRAM_CODE
    ,REGCODE_REGION_CODE
    ,S_BOUND_TIME
    ,THRESHOLD
    ,TOLL_INEQUITY_PROGRAM_FLAG
    ,TRIP_PLAZA
    ,TRUSTEE_TRUSTEE_ID
--    ,UTURN_INTERVAL   --  Not in Suntoll -- Required
    ,0 UTURN_INTERVAL
    ,VBTOL_EFFECTIVE_DATE
    ,VR_TOLL_PARTICIPATION
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_PLAZA; /*Change FTE_TABLE to the actual table name*/

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

  OPEN C1;   -- (v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PA_PLAZA_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_PA_PLAZA_tab.first .. DM_PA_PLAZA_tab.last
           INSERT INTO DM_PA_PLAZA VALUES DM_PA_PLAZA_tab(i);              

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

grant execute on DM_PA_PLAZA_PROC to public;
commit;


