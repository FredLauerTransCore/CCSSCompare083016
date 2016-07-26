/********************************************************
*
* Name: DM_REBILL_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_REBILL_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_REBILL_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_REBILL_INFO_TYP IS TABLE OF DM_REBILL_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_REBILL_INFO_tab DM_REBILL_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    aw.ACCT_NUM ACCOUNT_NUMBER 
    ,cc.CODE PAY_TYPE
    ,'XXXXXXXXXXXX'||substr(cc.CC_TOKEN,-4) CREDIT_CARD_NUMBER_MASK
    ,cc.CC_TOKEN CREDIT_CARD_NUMBER
    ,to_char(to_date(cc.EXPIRATION_DATE,'MM/YY'),'MM') CC_EXP_MONTH
    ,to_char(to_date(cc.EXPIRATION_DATE,'MM/YY'),'YYYY') CC_EXP_YEAR
    ,substr(cc.CC_TOKEN,-4) LAST_4_CC_NUMBER
    ,NULL ACH_BANK_ACCOUNT_NUMBER -- 'N/A'
    ,NULL ACH_BANK_ACCOUNT_NUMBER_MASK -- 'N/A'
    ,NULL ACH_BANK_ROUTING_NUMBER -- 'N/A'
    ,NULL LAST_4_ACH_NUMBER -- 'N/A'
    ,'1' SEQUENCE_NUMBER
    ,cc.CREATED_DATE CREATED --  NOT NULL Default ?
    ,aw.CREATED_BY CREATED_BY 
    ,SYSDATE LAST_UPD -- 'N/A'  NOT NULL Default ?
    ,NULL LAST_UPD_BY -- 'N/A'  Not Null
--    ,aw.LAST_USED_DATE LAST_USED_DATE  -- PA_ACCT_WALLET NOT NULL
    ,nvl(aw.LAST_USED_DATE,cc.CREATED_DATE) LAST_USED_DATE  -- PA_ACCT_WALLET
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_ACCT_WALLET aw
    ,PA_CREDIT_CARD cc
WHERE aw.ACCT_NUM >= p_begin_acct_num AND   aw.ACCT_NUM <= p_end_acct_num
and   aw.CARD_SEQ = cc.CARD_SEQ
    ; -- Source

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
    FETCH C1 BULK COLLECT INTO DM_REBILL_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN*/

    FOR i IN 1 .. DM_REBILL_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_REBILL_INFO_tab(i).ACCOUNT_NUMBER;
      end if;

      v_trac_etl_rec.track_last_val := DM_REBILL_INFO_tab(i).ACCOUNT_NUMBER;
      v_trac_etl_rec.end_val := DM_REBILL_INFO_tab(i).ACCOUNT_NUMBER;

    END LOOP;
    
    /* ETL SECTIONEND*/

    /*Bulk insert */ 
    FORALL i in DM_REBILL_INFO_tab.first .. DM_REBILL_INFO_tab.last
           INSERT INTO DM_REBILL_INFO VALUES DM_REBILL_INFO_tab(i);

    v_trac_etl_rec.end_val := v_trac_rec.end_acct;
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

grant execute on DM_REBILL_INFO_PROC to public;
commit;
