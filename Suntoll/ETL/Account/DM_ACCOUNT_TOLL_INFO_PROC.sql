/********************************************************
*
* Name: DM_ACCOUNT_TOLL_INFO_PROC
* Created by: RH, 5/20/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCOUNT_TOLL_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_ACCOUNT_TOLL_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_ACCOUNT_TOLL_INFO_TYP IS TABLE OF DM_ACCOUNT_TOLL_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_TOLL_INFO_tab DM_ACCOUNT_TOLL_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

-- EXTRACT RULE = SELECT * FROM PA_LANE_TXN WHERE TRANSPONDER_ID ENDS WITH '2010'  
/* indicates a violation toll */ -- (rentals included) 

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
--    UFM_LANE_TXN_INFO.HOST_UFM_TOKEN TX_EXTERN_REF_NO -- JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN
    nvl((select HOST_UFM_TOKEN from UFM_LANE_TXN_INFO where TXN_ID = lt.TXN_ID),0)
        TX_EXTERN_REF_NO   -- Required added default to 0 
    ,to_number(nvl(lt.EXT_MSG_SEQ_NUM,'0')) TX_SEQ_NUMBER  -- Required added default to 0 ?
    ,nvl(lt.TOUR_TOUR_SEQ,0) EXTERN_FILE_ID -- Required added default to 0 ?
--    ,NVL(lt.EXT_LANE_ID,1) LANE_ID
    ,1 LANE_ID
    ,lt.EXT_DATE_TIME TX_TIMESTAMP
    ,CASE WHEN lt.MSG_ID IN ('TCA','XADJ','FADJ') 
          THEN 1 ELSE 0
      END TX_MOD_SEQ   -- IF MSG_ID IN ('TCA','XADJ','FADJ') THEN 1 ELSE 0
    ,'V' TX_TYPE_IND
    ,'I' TX_SUBTYPE_IND
    ,DECODE(substr(pp.PLAZA_ID,1,3),'004', 'C', 'B')
        TOLL_SYSTEM_TYPE -- IF PLAZA_ID STARTS WITH '004' THEN 'C' ELSE 'B'
--    ,to_number(nvl(lt.EXT_LANE_TYPE_CODE,'0')) LANE_MODE  Source is char, target is number
    ,0 LANE_MODE  -- Required added default to 0 
    ,1 LANE_TYPE
    ,0 LANE_STATE
    ,0 LANE_HEALTH
--    AGENCY_ID.PLAZA_AGENCY_ID -- Join ST_INTEROP_AGENCIES and PA_PLAZA on ENT_PLAZA_ID to PLAZA_ID
    ,to_number(nvl((select AGENCY_ID from ST_INTEROP_AGENCIES where ENT_PLAZA_ID = pp.PLAZA_ID),'0'))
        PLAZA_AGENCY_ID  -- Required added default to 0 
    ,to_number(nvl(lt.EXT_PLAZA_ID,'0')) PLAZA_ID -- Required added default to 0 
    ,0 COLLECTOR_ID
    ,0 TOUR_SEGMENT_ID
    ,0 ENTRY_DATA_SOURCE
    ,to_number(nvl(lt.ENT_LANE_ID,'0')) ENTRY_LANE_ID  -- Required added default to 0 
    ,to_number(nvl(lt.ENT_PLAZA_ID,'0')) ENTRY_PLAZA_ID  -- Required added default to 0 
    ,nvl(lt.ENT_DATE_TIME,SYSDATE) ENTRY_TIMESTAMP   -- Required added default to SYSDATE
    ,nvl(lt.TXN_ENTRY_NUMBER,0) ENTRY_TX_SEQ_NUMBER  -- Required added default to 0 
    ,0 ENTRY_VEHICLE_SPEED
    ,0 LANE_TX_STATUS
    ,0 LANE_TX_TYPE
    ,0 TOLL_REVENUE_TYPE
    ,to_number(nvl(lt.AVC_CLASS,'0')) ACTUAL_CLASS  -- Required added default to 0 
    ,to_number(nvl(lt.AVC_CLASS,'0')) ACTUAL_AXLES  -- Required added default to 0 
    ,0 ACTUAL_EXTRA_AXLES
    ,0 COLLECTOR_CLASS
    ,0 COLLECTOR_AXLES
    ,0 PRECLASS_CLASS
    ,0 PRECLASS_AXLES
    ,0 POSTCLASS_CLASS
    ,0 POSTCLASS_AXLES
    ,0 FORWARD_AXLES
    ,0 REVERSE_AXLES
    ,nvl(lt.TOLL_AMT_FULL,0) FULL_FARE_AMOUNT  -- Required added default to 0
    ,nvl(lt.TOLL_AMT_CHARGED,0) DISCOUNTED_AMOUNT  -- Required added default to 0
    ,nvl(lt.TOLL_AMT_COLLECTED,0) COLLECTED_AMOUNT  -- Required added default to 0
    ,0 UNREALIZED_AMOUNT
    ,'N' IS_DISCOUNTABLE
    ,'N' IS_MEDIAN_FARE
    ,'N' IS_PEAK
    ,0 PRICE_SCHEDULE_ID
    ,0 VEHICLE_SPEED
    ,0 RECEIPT_ISSUED
    ,lt.TRANSP_ID DEVICE_NO
    ,(select to_number(pa.ACCTTYPE_ACCT_TYPE_CODE) from PA_ACCT pa where pa.ACCT_NUM=kl.ACCT_NUM)
        ACCOUNT_TYPE -- PA_ACCT
    ,to_number(nvl(lt.TRANSP_CLASS,'0')) DEVICE_CODED_CLASS  -- Required added default to 0/Convert to Number from varchar
    ,0 DEVICE_AGENCY_CLASS
    ,0 DEVICE_IAG_CLASS
    ,0 DEVICE_AXLES
    ,kl.ACCT_NUM ETC_ACCOUNT_ID  -- KS_LEDGER - Join KS_LEDGER on TXN_ID
    ,0 ACCOUNT_AGENCY_ID
    ,to_number(nvl(lt.TRANSP_CLASS,'0'))  READ_AVI_CLASS  -- Required added default to 0
    ,0 READ_AVI_AXLES
    ,'N' DEVICE_PROGRAM_STATUS
    ,'N' BUFFERED_READ_FLAG
    ,to_number(NVL(MSG_INVALID,'0')) LANE_DEVICE_STATUS -- Required added default to 0
    ,0 POST_DEVICE_STATUS
    ,(nvl(kl.BALANCE,0)-nvl(kl.AMOUNT,0)) PRE_TXN_BALANCE -- Derived - KS_LEDGER current balance minus amount 

------ PLAN_TYPE_ID - IF ITOL_AGENCY_CODE IN (9,10,11,12,13,14,15) 
------ THEN RENTAL ELSE POSTPAID, 
--  Xerox will provide numbers for  rental vs postpaid  ---------------------
--    ,CASE WHEN ITOL_AGENCY_CODE IN (9,10,11,12,13,14,15) 
--          THEN 9  --'RENTAL' 
--          ELSE 10 --'POSTPAID'
--      END PLAN_TYPE_ID
      ,0 PLAN_TYPE_ID
 -- IF RENTAL THEN 9 of POSTPAID THEN 10 (From Above)  ?? Required
--    ,CASE WHEN ITOL_AGENCY_CODE IN (9,10,11,12,13,14,15) 
--          THEN 9  --'RENTAL' 
--          ELSE 10 --'POSTPAID'
--      END ETC_TX_STATUS  --   
      ,0 ETC_TX_STATUS  --   

    ,'F' SPEED_VIOL_FLAG
    ,'Y' IMAGE_TAKEN
-- PLATE_COUNTRY - LOOKUP PA_STATE_CODE.  (visit DM_VEHICLE_INFO and DM_ACCT_INFO)
-- Xerox - internal lookup in DM_ADDRESS_INFO and assign country correctly. 
--    ,NULL PLATE_COUNTRY -- - LOOKUP PA_STATE_CODE - in ETL
    ,nvl((select substr(csl.COUNTRY,1,4) from COUNTRY_STATE_LOOKUP csl
        where csl.STATE_ABBR = lt.STATE_ID_CODE),'USA')
        PLATE_COUNTRY

--    ,(select STATE_CODE_ABBR from PA_STATE_CODE where STATE_CODE_NUM=lt.STATE_ID_CODE)
--        PLATE_STATE  -- JOIN TO PA_STATE_CODE RETURN STATE_CODE_ABBR
    ,nvl(lt.STATE_ID_CODE,'n') PLATE_STATE  -- Required
    ,nvl(lt.VEH_LIC_NUM,'NULL-none') PLATE_NUMBER
    ,trunc(EXT_DATE_TIME) REVENUE_DATE -- Date only 
    ,trunc(TXN_PROCESS_DATE) POSTED_DATE -- Date only
    ,0 ATP_FILE_ID
    ,TXN_PROCESS_DATE UPDATE_TS -- Date and Time
    ,'N' IS_REVERSED
    ,0 CORR_REASON_ID
    ,trunc(TXN_PROCESS_DATE) RECON_DATE -- Date only
    ,0 RECON_STATUS_IND  -- XEROX - TO FOLLOW UP
    ,0 RECON_SUB_CODE_IND  -- XEROX - TO FOLLOW UP
    ,nvl((select pt.COLLECTION_DATE from PA_TOUR pt where pt.TOUR_SEQ = lt.TOUR_TOUR_SEQ),SYSDATE)  -- (SYSDATE-(352*100)
        EXTERN_FILE_DATE  -- JOIN TO PA_TOUR ON TOUR_SEQ=TOUR_TOUR_SEQ RETURN COLLECTION_DATE
    ,0 MILEAGE
    ,0 DEVICE_READ_COUNT
    ,0 DEVICE_WRITE_COUNT
    ,0 ENTRY_DEVICE_READ_COUNT
    ,0 ENTRY_DEVICE_WRITE_COUNT
    ,'000000000000' DEPOSIT_ID
    ,trunc(EXT_DATE_TIME) TX_DATE -- Date only
    ,nvl(substr(trim(pp.PLAZA_NAME),1,15),'NULL-none') LOCATION  -- JOIN PA_PLAZA ON PLAZA_ID=EXT_PLAZA_ID RETURN PLAZA_NAME
    ,nvl(pp.FACCODE_FACILITY_CODE,'NULL-none') FACILITY_ID -- JOIN PA_PLAZA ON PLAZA_ID=EXT_PLAZA_ID RETURN FACCODE_FACILITY_CODE
    ,0 FARE_TBL_ID
    ,0 TRAN_CODE
    ,'SUNTOLL' SOURCE_SYSTEM
    ,lt.TXN_ID LANE_TX_ID
    ,lt.TRANSP_INTERNAL_NUM DEVICE_INTERNAL_NUMBER
FROM PA_LANE_TXN lt
    ,KS_LEDGER kl
    ,PA_PLAZA pp
WHERE lt.txn_id = kl.PA_LANE_TXN_ID (+)
AND lt.EXT_PLAZA_ID = pp.PLAZA_ID (+)
AND lt.TRANSP_ID like '%2010'     --AND TRANSPONDER_ID ENDS WITH '2010' 
AND  kl.ACCT_NUM >= p_begin_acct_num AND   kl.ACCT_NUM <= p_end_acct_num
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
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_TOLL_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /*ETL SECTION BEGIN*/
    
    FOR i in DM_ACCOUNT_TOLL_INFO_tab.first .. DM_ACCOUNT_TOLL_INFO_tab.last loop
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_ACCOUNT_TOLL_INFO_tab(i).ACCOUNT_NUMBER;
      end if;

      begin
        select COUNTRY into DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_COUNTRY 
        from COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_ACCOUNT_TOLL_INFO_tab(i).STATE_ID_CODE
        and rownum<=1;
      exception
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_COUNTRY:='USA';
      end;
      
      v_trac_etl_rec.track_last_val := DM_ACCOUNT_TOLL_INFO_tab(i).ACCOUNT_NUMBER;     
    end loop;
    
      /*ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_TOLL_INFO_tab.first .. DM_ACCOUNT_TOLL_INFO_tab.last
           INSERT INTO DM_ACCOUNT_TOLL_INFO VALUES DM_ACCOUNT_TOLL_INFO_tab(i);
                       
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

grant execute on DM_ACCOUNT_TOLL_INFO_PROC to public;
commit;


