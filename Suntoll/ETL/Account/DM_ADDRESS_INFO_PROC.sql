/********************************************************
*
* Name: DM_ADDRESS_INFO_PROC
* Created by: RH, 5/16/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ADDRESS_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_ADDRESS_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_ADDRESS_INFO_TYP IS TABLE OF DM_ADDRESS_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ADDRESS_INFO_tab DM_ADDRESS_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,'MAILING' ADDR_TYPE
    ,1 ADDR_TYPE_INT_ID
    ,substr(trim(ADDR1),1,40) STREET_1    -- ADDR_1 mapping
    ,trim(ADDR2) STREET_2    -- ADDR_2 mapping
    ,substr(trim(CITY),1,25) CITY
    ,STATE_CODE_ABBR STATE
    ,ZIP_CODE ZIP_CODE
    ,SUBSTR(ZIP_CODE,7,10) ZIP_PLUS4
    ,NULL COUNTRY   -- COUNTRY_COUNTRY_CODE mapping in ETL
    ,nvl2(BAD_ADDR_DATE,'Y','N') NIXIE   -- in ETL
    ,to_date('02/27/2017', 'MM/DD/YYYY') NIXIE_DATE
    ,'N' NCOA_FLAG
    ,'N' ADDRESS_CLEANSED_FLG
    ,'CSC' ADDRESS_SOURCE
--    ,(select pa.CREATED_ON from PA_ACCT pa
--          WHERE pa.ACCT_NUM = ACCT_NUM)  CREATED
    ,NULL  CREATED
    ,'SUNTOLL_CSC_ID' CREATED_BY
    ,to_date('02/27/2017', 'MM/DD/YYYY') LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
    ,NULL ADDRESS_NUMBER  -- DECODE?
-- ICD - ,(select csl.COUNTY from COUNTY_STATE_LOOKUP csl where csl.CITY = CITY) COUNTY_CODE
    ,COUNTY_CODE   COUNTY_CODE  --  (Need table)
--    ,NULL   COUNTY_CODE  --  (Need table)
FROM PA_ACCT_ADDR
WHERE DEFAULT_ADDR_FLAG = 'Y'
and   ACCT_NUM >= p_begin_acct_num   AND   ACCT_NUM <= p_end_acct_num
;

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

  OPEN C1(v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ADDRESS_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN*/
    
    FOR i in DM_ADDRESS_INFO_tab.first .. DM_ADDRESS_INFO_tab.last loop
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_ADDRESS_INFO_TAB(i).ACCOUNT_NUMBER;
      end if;

      begin
        select COUNTRY into DM_ADDRESS_INFO_tab(i).COUNTRY 
        from COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_ADDRESS_INFO_tab(i).STATE
        and rownum<=1;
      exception
        when others then null;
        DM_ADDRESS_INFO_tab(i).COUNTRY:='USA';
      end;
      
     begin
        select CASE WHEN (MAIL_RETURNED = 0 or MAIL_RETURNED is NULL) 
                    THEN 'Y' ELSE 'N'
              END
        into  DM_ADDRESS_INFO_tab(i).NIXIE 
        from  PA_ACCT_FLAGS 
        where ACCT_ACCT_NUM = DM_ADDRESS_INFO_tab(i).ACCOUNT_NUMBER
        and rownum<=1;
      exception
        when others then null;
        DM_ADDRESS_INFO_tab(i).NIXIE:='Y';
      end;
      
      v_trac_etl_rec.track_last_val := DM_ADDRESS_INFO_TAB(i).ACCOUNT_NUMBER;     
    end loop;
    
      /*ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ADDRESS_INFO_tab.first .. DM_ADDRESS_INFO_tab.last
           INSERT INTO DM_ADDRESS_INFO VALUES DM_ADDRESS_INFO_tab(i);

    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    v_trac_etl_rec.end_val := v_trac_etl_rec.track_last_val;
    update_track_proc(v_trac_etl_rec);
    COMMIT;
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Total load count : '||ROW_CNT);

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := v_trac_etl_rec.result_code||SQLCODE;
  v_trac_etl_rec.result_msg := v_trac_etl_rec.result_msg||SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  EXCEPTION
  WHEN OTHERS THEN
    v_trac_etl_rec.result_code := v_trac_etl_rec.result_code||SQLCODE;
    v_trac_etl_rec.result_msg := v_trac_etl_rec.result_msg||SQLERRM;
    v_trac_etl_rec.proc_end_date := SYSDATE;
    update_track_proc(v_trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS

commit;
