/********************************************************
*
* Name: DM_AWAY_AGENCY_TX_IMG_INFO_PROC
* Created by: RH, 7/29/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_AWAY_AGENCY_TX_IMG_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_AWAY_AGENCY_TX_IMG_INF_PROC 
(i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_AWAY_AGENCY_TX_IMG_INFO_TYP IS TABLE OF DM_AWAY_AGENCY_TX_IMG_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_AWAY_AGENCY_TX_IMG_INFO_tab DM_AWAY_AGENCY_TX_IMG_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;

--EXTRACT RULE = SELECT * FROM PA_LANE_TXN WHERE TRANSPONDER_ID DOES NOT END WITH '2010'  

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
-- JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN
--    nvl((select u.HOST_UFM_TOKEN from UFM_LANE_TXN_INFO u where u.TXN_ID = lt.TXN_ID),0)
    0 TX_EXTERN_REF_NO   -- Required added default to 0 if NULL
    ,nvl(EXT_MSG_SEQ_NUM,0) TX_SEQ_NUMBER  -- Required default to 0 if NULL
    ,0 EXTERN_FILE_ID
    ,EXT_LANE_ID LANE_ID
    ,EXT_DATE_TIME TX_TIMESTAMP
    ,CASE WHEN MSG_ID IN ('TCA','XADJ','FADJ') 
          THEN 1 ELSE 0
     END TX_MOD_SEQ
    ,'V' TX_TYPE_IND
    ,'F' TX_SUBTYPE_IND
 
-- Derived IF PLAZA_ID STARTS WITH '004' THEN 'C' ELSE 'B'
    ,DECODE(substr(trim(EXT_PLAZA_ID),1,3),'004', 'C', 'B') TOLL_SYSTEM_TYPE 
    ,decode(EXT_LANE_TYPE_CODE   -- Not a number
        ,'A', 100  --  AC Lane 100
        ,'E', 101  --  AE Lane 101
        ,'M', 102  --  MB Lane 102
        ,'D', 103  --  Dedicated 103
        ,'C', 104  --  AC-AVI 104
        ,'B', 105  --  MB-AVI 105
        ,'N', 106  --  ME Lane 106
        ,'X', 107  --  MX Lane 107
        ,'F', 108  --  Free Lane 108
        ,'Y', 109  --  Dedicated Entry 109
        ,'Z', 110  --  Dedicated Exit 110
        ,'S', 111  --  Express Lane 111
        ,'G', 112  --  Magic Lane 112
        ,'Q', 113  --  APM Lane 113
        ,'T', 114  --  MA Lane 114
        ,0) LANE_MODE
    ,1 LANE_TYPE
    ,0 LANE_STATE
    ,0 LANE_HEALTH  
 -- PA_PLAZA.AGENCY_ID Join ST_INTEROP_AGENCIES and PA_PLAZA on ENT_PLAZA_ID to PLAZA_ID
    ,'0' PLAZA_AGENCY_ID   -- in ETL  
    
    ,EXT_PLAZA_ID PLAZA_ID
    ,0 COLLECTOR_ID
    ,0 TOUR_SEGMENT_ID
    ,'0' ENTRY_DATA_SOURCE
    
--    ,ENT_LANE_ID ENTRY_LANE_ID
--    ,ENT_PLAZA_ID ENTRY_PLAZA_ID
--    ,ENT_DATE_TIME ENTRY_TIMESTAMP
--    ,TXN_ENTRY_NUMBER ENTRY_TX_SEQ_NUMBER
--    ,to_number(nvl(ENT_LANE_ID,'0')) ENTRY_LANE_ID  -- Required - If NULL default ?
--    ,to_number(nvl(ENT_PLAZA_ID,'0')) ENTRY_PLAZA_ID  -- Required - If NULL default ?
    ,nvl(ENT_LANE_ID,'0') ENTRY_LANE_ID  -- Required - If NULL default ?
    ,nvl(ENT_PLAZA_ID,'0') ENTRY_PLAZA_ID  -- Required - If NULL default ?
    ,nvl(ENT_DATE_TIME, to_date('01-01-1900','MM-DD-YYYY')) ENTRY_TIMESTAMP  -- Required - If NULL default ?
    ,nvl(TXN_ENTRY_NUMBER,0) ENTRY_TX_SEQ_NUMBER  -- Required - If NULL default ?

    ,0 ENTRY_VEHICLE_SPEED
    ,0 LANE_TX_STATUS
    ,0 LANE_TX_TYPE
    ,0 TOLL_REVENUE_TYPE
    ,AVC_CLASS ACTUAL_CLASS
    ,AVC_CLASS ACTUAL_AXLES
    ,0 ACTUAL_EXTRA_AXLES
    ,0 COLLECTOR_CLASS
    ,0 COLLECTOR_AXLES
    ,0 PRECLASS_CLASS
    ,0 PRECLASS_AXLES
    ,0 POSTCLASS_CLASS
    ,0 POSTCLASS_AXLES
    ,0 FORWARD_AXLES
    ,0 REVERSE_AXLES
    ,nvl(TOLL_AMT_FULL,0) FULL_FARE_AMOUNT
    ,nvl(TOLL_AMT_CHARGED,0) DISCOUNTED_AMOUNT
    ,nvl(TOLL_AMT_COLLECTED,0) COLLECTED_AMOUNT
    ,0 UNREALIZED_AMOUNT
    ,'N' IS_DISCOUNTABLE
    ,'N' IS_MEDIAN_FARE
    ,'N' IS_PEAK
    ,0 PRICE_SCHEDULE_ID
    ,0 VEHICLE_SPEED
    ,0 RECEIPT_ISSUED
    
-- JOIN WITH JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN AND 
-- JOIN UFM_TOKEN TO UFM_ID FOR VIOLATIONS_EVENTS_AUX
    ,'***'  DEVICE_NO -- VIOLATIONS_EVENTS_AUX.TT_ID   ???
    ,0 ACCOUNT_TYPE  -- Convert to Number from varchar
--    ,to_number(nvl(TRANSP_CLASS,'0')) DEVICE_CODED_CLASS  -- Required  
    ,to_number(nvl(TRANSP_CLASS,'0')) DEVICE_CODED_CLASS  -- Required  
    ,0 DEVICE_AGENCY_CLASS
    ,0 DEVICE_IAG_CLASS
    ,0 DEVICE_AXLES
    ,0 ETC_ACCOUNT_ID   -- ACCT_NUM ??
    
-- SUBSTR(TRANSP_ID,9,2) JOIN ST_INTEROP_AGENCIES ON AGENCY_CODE RETURN AGENCY_ID 
-- ACCOUNT_AGENCY_ID -- SUBSTR(TRANSP_ID,9,2)
    ,SUBSTR(TRANSP_ID,9,2) ACCOUNT_AGENCY_ID  -- Use to get in ETL
 
--    ,to_number(nvl(TRANSP_CLASS,'0')) READ_AVI_CLASS
    ,nvl(TRANSP_CLASS,'0') READ_AVI_CLASS
    ,0 READ_AVI_AXLES
    ,'N' DEVICE_PROGRAM_STATUS
    ,'N' BUFFERED_READ_FLAG
    ,nvl(MSG_INVALID,'0') LANE_DEVICE_STATUS
    ,0 POST_DEVICE_STATUS
    ,0 PRE_TXN_BALANCE
    ,1 PLAN_TYPE_ID
    ,6 ETC_TX_STATUS
    ,'N' SPEED_VIOL_FLAG
    ,'Y' IMAGE_TAKEN
    ,'USA' PLATE_COUNTRY  -- in ETL
    ,STATE_ID_CODE PLATE_STATE
    ,VEH_LIC_NUM PLATE_NUMBER
    ,trunc(EXT_DATE_TIME) REVENUE_DATE
    ,trunc(TXN_PROCESS_DATE) POSTED_DATE
    ,0 ATP_FILE_ID
    ,TXN_PROCESS_DATE UPDATE_TS
    ,'0' IS_REVERSED
    ,0 CORR_REASON_ID
    ,trunc(TXN_PROCESS_DATE) RECON_DATE
    ,0 RECON_STATUS_IND
    ,0 RECON_SUB_CODE_IND
    ,to_date('01-01-1900','MM-DD-YYYY') EXTERN_FILE_DATE
    ,0 MILEAGE
    ,0 DEVICE_READ_COUNT
    ,0 DEVICE_WRITE_COUNT
    ,0 ENTRY_DEVICE_READ_COUNT
    ,0 ENTRY_DEVICE_WRITE_COUNT
    ,'000000000000' DEPOSIT_ID
    ,trunc(EXT_DATE_TIME) TX_DATE
    ,EXT_PLAZA_ID LOCATION
    ,0 FARE_TBL_ID
    ,nvl(EXT_MSG_SEQ_NUM,0) LANE_SEQ_NO
    ,0 HUB_REF_ID
    ,TXN_ID LANE_TX_ID
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_LANE_TXN lt
where txn_id = (select PA_LANE_TXN_ID
                from   KS_LEDGER kl2
                WHERE kl2.PA_LANE_TXN_ID = lt.txn_id
                AND   kl2.ACCT_NUM >= p_begin_acct_num AND   kl2.ACCT_NUM <= p_end_acct_num
                and   rownum<=1)
and TRANSP_ID NOT like '%2010'  --TRANSPONDER_ID DOES NOT END WITH '2010'   
;   -- source

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
    FETCH C1 BULK COLLECT INTO DM_AWAY_AGENCY_TX_IMG_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN*/
    FOR i IN 1 .. DM_AWAY_AGENCY_TX_IMG_INFO_tab.COUNT LOOP

      begin
        select  ACCT_NUM
        into    DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).ETC_ACCOUNT_ID
        from    KS_LEDGER -- k1
        where   PA_LANE_TXN_ID=DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).TX_EXTERN_REF_NO
        and     rownum<=1
--        and     POSTED_DATE = (select max(POSTED_DATE) 
--                              from KS_LEDGER -- k2
--                              where   PA_LANE_TXN_ID=DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).TX_EXTERN_REF_NO)
----                              where k2.id = k1.id)
        ;
      exception 
        when others then null;
        DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).ETC_ACCOUNT_ID := 0;
      end;
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).ETC_ACCOUNT_ID;
      end if;

      /* get UFM_LANE_TXN_INFO.HOST_UFM_TOKEN for TX_EXTERN_REF_NO */
-- JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN
      begin
        select HOST_UFM_TOKEN 
        into  DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).TX_EXTERN_REF_NO 
        from  UFM_LANE_TXN_INFO 
        where TXN_ID=DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).LANE_TX_ID
--              and rownum<=1
        ;
        exception 
          when others then null;
          DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).TX_EXTERN_REF_NO:=0;
      end;

      /* get ST_INTEROP_AGENCIES.AGENCY_ID for PLAZA_AGENCY_ID */
-- Agency  id to which the plaza and lane belong to where the transaction took place.
      begin
        select sia.AGENCY_ID 
        into  DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).PLAZA_AGENCY_ID 
        from  PA_PLAZA pp, ST_INTEROP_AGENCIES sia
        Where pp.plaza_id  = DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).PLAZA_ID
        and   pp.AUTHCODE_AUTHORITY_CODE = sia.AUTHORITY_CODE
        and   rownum<=1
        ;
      exception
        when no_data_found then null;
          DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).PLAZA_AGENCY_ID:='0'; 
        when others then null;
          DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).PLAZA_AGENCY_ID:='0';
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
           DBMS_OUTPUT.PUT_LINE('PLAZA_AGENCY_ID ERROR CODE: '||v_trac_etl_rec.result_code);
           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;     
      
------ JOIN WITH JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN AND 
------ JOIN UFM_TOKEN TO UFM_ID FOR EVENT_ROV       
      begin
        select nvl(er.TRANSP_ID, '***')
        into  DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).DEVICE_NO
        from  EVENT_ROV er, 
              UFM_LANE_TXN_INFO ult
        where er.UFM_ID = ult.HOST_UFM_TOKEN 
        and   ult.TXN_ID = DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).TX_EXTERN_REF_NO   
        and   rownum<=1
        ;
      exception
        when others then null;
        DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).DEVICE_NO := '***';
      end;

------ VIOLATIONS_EVENTS_AUX.TT_ID
--JOIN WITH JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN AND 
--JOIN UFM_TOKEN TO UFM_ID FOR VIOLATIONS_EVENTS_AUX
--      begin
--        select TT_ID
--        into  DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).DEVICE_NO
--        from  VIOLATIONS_EVENTS_AUX er, 
--              UFM_LANE_TXN_INFO ult
--        where er.UFM_ID = ult.HOST_UFM_TOKEN 
--        and   ult.TXN_ID = DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).TX_EXTERN_REF_NO   
--        and   rownum<=1
--        ;
--      exception
--        when others then null;
--        DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).DEVICE_NO := '***';
--      end;


------------ ACCOUNT_AGENCY_ID - SUBSTR(TRANSP_ID,9,2) 
----SUBSTR(TRANSP_ID,9,2) JOIN ST_INTEROP_AGENCIES ON AGENCY_CODE 
----RETURN AGENCY_ID 
--      begin
--        select distinct AGENCY_ID 
--        into  DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).ACCOUNT_AGENCY_ID 
--        from  ST_INTEROP_AGENCIES sia
--        Where AGENCY_CODE  = DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).ACCOUNT_AGENCY_ID
--        and   rownum<=1
--        ;
--      exception
--        when no_data_found then null;
--          DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).ACCOUNT_AGENCY_ID:='0'; 
--        when others then null;
--          DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).ACCOUNT_AGENCY_ID:='0'; 
--          v_trac_etl_rec.result_code := SQLCODE;
--          v_trac_etl_rec.result_msg := SQLERRM;
--          v_trac_etl_rec.proc_end_date := SYSDATE;
--          update_track_proc(v_trac_etl_rec);
--           DBMS_OUTPUT.PUT_LINE('ACCOUNT_AGENCY_ID ERROR CODE: '||v_trac_etl_rec.result_code);
--           DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
--      end;


--LOOKUP PA_STATE_CODE. Xerox - internal lookup in DM_ADDRESS_INFO and assign country correctly. 
      begin
        select COUNTRY into DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).PLATE_COUNTRY 
        from  COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).PLATE_STATE;
      exception
        when others then null;
        DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).PLATE_COUNTRY:='USA';
      end;
      
      v_trac_etl_rec.track_last_val := DM_AWAY_AGENCY_TX_IMG_INFO_tab(i).ETC_ACCOUNT_ID;
    END LOOP;
    /*  ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_AWAY_AGENCY_TX_IMG_INFO_tab.first .. DM_AWAY_AGENCY_TX_IMG_INFO_tab.last
           INSERT INTO DM_AWAY_AGENCY_TX_IMG_INFO VALUES DM_AWAY_AGENCY_TX_IMG_INFO_tab(i);
                       
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

commit;


