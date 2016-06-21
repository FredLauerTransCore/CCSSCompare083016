/********************************************************
*
* Name: DM_NONFIN_ACT_INFO_PROC
* Created by: RH, 6/4/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_NONFIN_ACT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- EMP_CODE IS LIKE '99xx' or '0100' -- SYSTEM
-- RH 6/20/2016 Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_NONFIN_ACT_INFO_PROC   
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS


TYPE DM_NONFIN_ACT_INFO_TYP IS TABLE OF DM_NONFIN_ACT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_NONFIN_ACT_INFO_tab DM_NONFIN_ACT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,nvl((SELECT MAX(ACTIVITY_NUMBER + 1) 
        FROM DM_NONFIN_ACT_INFO 
        WHERE ACCT_NUM = NOTE.ACCT_NUM),0) as ACTIVITY_NUMBER
--    ,NULL ACTIVITY_NUMBER
    ,TYPE_CODE CATEGORY
    ,PROB_CODE SUB_CATEGORY
    ,TYPE_ID ACTIVITY_TYPE
    ,NOTE DESCRIPTION
    ,CREATED CREATED
    ,EMP_CODE CREATED_BY
    ,CREATED LAST_UPD
    ,EMP_CODE LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
FROM NOTE
where EMP_CODE like '99%' or EMP_CODE = '0100'
--AND  ACCT_NUM >= p_begin_acct_num AND   ACCT_NUM <= p_end_acct_num
; 
--

SQL_STRING  varchar2(500) := 'delete table ';

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
    FETCH C1 BULK COLLECT INTO DM_NONFIN_ACT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_NONFIN_ACT_INFO_tab.first .. DM_NONFIN_ACT_INFO_tab.last
           INSERT INTO DM_NONFIN_ACT_INFO VALUES DM_NONFIN_ACT_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total ROW_CNT : '||ROW_CNT);

  COMMIT;

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  
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

