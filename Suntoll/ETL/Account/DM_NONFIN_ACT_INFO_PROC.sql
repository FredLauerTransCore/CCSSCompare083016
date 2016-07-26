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

-- EXTRACT RULE - EMP_CODE IS LIKE '99xx' or '0100' -- SYSTEM
-- RH 6/20/2016 Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_NONFIN_ACT_INFO_PROC   
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS


TYPE DM_NONFIN_ACT_INFO_TYP IS TABLE OF DM_NONFIN_ACT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_NONFIN_ACT_INFO_tab DM_NONFIN_ACT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=1000;


CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,0 ACTIVITY_NUMBER  -- In ETL
    ,TYPE_CODE CATEGORY
    ,PROB_CODE SUB_CATEGORY
    ,TYPE_ID ACTIVITY_TYPE
    ,trim(NOTE) DESCRIPTION
    ,CREATED CREATED
    ,EMP_CODE CREATED_BY
    ,CREATED LAST_UPD
    ,EMP_CODE LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
FROM NOTE
where (EMP_CODE like '99%' or EMP_CODE = '0100')
--and ACCT_NUM is not null
AND  ACCT_NUM >= p_begin_acct_num AND   ACCT_NUM <= p_end_acct_num
and  ACCT_NUM >0
; 
--

SQL_STRING  varchar2(500) := 'delete table ';

v_activity_number DM_NONFIN_ACT_INFO.ACTIVITY_NUMBER%TYPE := 0;
row_cnt           NUMBER := 0;
v_trac_rec        dm_tracking%ROWTYPE;
v_trac_etl_rec    dm_tracking_etl%ROWTYPE;

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

  OPEN C1(v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  begin
    select nvl(max(ACTIVITY_NUMBER),0) into  v_activity_number
    from DM_NONFIN_ACT_INFO
    ;
  exception 
    when no_data_found then --null;
      v_activity_number :=  0;
    when others then --null;
      v_activity_number :=  0;
      DBMS_OUTPUT.PUT_LINE('activity_number ERROR CODE: '||SQLCODE);
      DBMS_OUTPUT.PUT_LINE('activity_number ERROR MSG: '||SQLERRM);
  end;    
    
  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_NONFIN_ACT_INFO_tab
    LIMIT P_ARRAY_SIZE;

--ETL SECTION BEGIN mapping
--DBMS_OUTPUT.PUT_LINE(' - max activity_number: '||v_activity_number);

    FOR i IN 1 .. DM_NONFIN_ACT_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_NONFIN_ACT_INFO_tab(i).ACCOUNT_NUMBER;
        DBMS_OUTPUT.PUT_LINE('activity_number: '||v_activity_number);
    
      end if;
      
      v_activity_number := v_activity_number+1;
      DM_NONFIN_ACT_INFO_tab(i).activity_number :=  v_activity_number;
      
      v_trac_etl_rec.track_last_val := DM_NONFIN_ACT_INFO_tab(i).ACCOUNT_NUMBER;
      v_trac_etl_rec.end_val := DM_NONFIN_ACT_INFO_tab(i).ACCOUNT_NUMBER;

    END LOOP;
--      ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_NONFIN_ACT_INFO_tab.first .. DM_NONFIN_ACT_INFO_tab.last
           INSERT INTO DM_NONFIN_ACT_INFO VALUES DM_NONFIN_ACT_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
    COMMIT;
                       
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

grant execute on DM_NONFIN_ACT_INFO_PROC to public;
commit;


