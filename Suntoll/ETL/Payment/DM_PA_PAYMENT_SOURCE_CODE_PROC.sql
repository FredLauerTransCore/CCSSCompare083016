/********************************************************
*
* Name: DM_PA_PAYMENT_SOURCE_CODE_PROC
* Created by: RH, 6/1/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PA_PAYMENT_SOURCE_CODE
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PA_PAYMENT_SOURCE_CODE_PROC  
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_PA_PAYMENT_SOURCE_CODE_TYP IS TABLE OF DM_PA_PAYMENT_SOURCE_CODE%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PA_PAYMENT_SOURCE_CODE_tab DM_PA_PAYMENT_SOURCE_CODE_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1 IS SELECT 
    PAYMENT_SOURCE_CODE
    ,PAYMENT_SOURCE_DESC
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_PAYMENT_SOURCE_CODE;

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
    FETCH C1 BULK COLLECT INTO DM_PA_PAYMENT_SOURCE_CODE_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_PA_PAYMENT_SOURCE_CODE_tab.first .. DM_PA_PAYMENT_SOURCE_CODE_tab.last
           INSERT INTO DM_PA_PAYMENT_SOURCE_CODE VALUES DM_PA_PAYMENT_SOURCE_CODE_tab(i);              

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

grant execute on DM_PA_PAYMENT_SOURCE_CODE_PROC to public;
commit;



