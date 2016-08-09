/********************************************************
*
* Name: DM_VIOL_IMAGE_TX_INFO_PROC
* Created by: RH, 7/29/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_VIOL_IMAGE_TX_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_VIOL_IMAGE_TX_INFO_PROC
(i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS


TYPE DM_VIOL_IMAGE_TX_INFO_TYP IS TABLE OF DM_VIOL_IMAGE_TX_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_VIOL_IMAGE_TX_INFO_tab DM_VIOL_IMAGE_TX_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;

-- EXTRACT RULE -  
-- JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN AND 
-- JOIN UFM_TOKEN TO UFM_ID FOR EVENT_ROV

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    EXT_LANE_ID LANE_ID
    ,EXT_DATE_TIME TX_TIMESTAMP
    ,nvl(EXT_MSG_SEQ_NUM,0) TX_SEQ_NUMBER -- Required 
    ,VPS_EVENT_ID IN_FILE_NAME
    
    ,'**' OUT_FILE_NAME
    ,'**' PROCESS_DESC
    ,trunc(lt.EXT_DATE_TIME) RECEIVE_DATE
    ,trunc(lt.EXT_DATE_TIME) PROCESS_DATE

-- JOIN VPS_EVENT_ID of EVENT_ROV to VIOLATION_EVENT_ID of LPR_RESULTS
    ,NULL OCR_CONF          -- in ETL
    ,NULL OCR_PLATE_STATE   -- in ETL
    ,NULL OCR_PLATE_NUMBER  -- in ETL
    
    ,TXN_ID VEH_SEQ_NUMBER
    ,1 LANE_IMAGE_CNT
    ,0 PROC_IMAGE_CNT
    ,EXT_DATE_TIME FTP_TIMESTAMP
    ,EXT_DATE_TIME UPDATE_TS
    ,NULL OCR_STATUS    -- in ETL IMAGE_VERIFIED IF = 1 then 0 else 1
    ,trunc(EXT_DATE_TIME) TX_DATE -- DATE PORTION ONLY
    ,TXN_ID LANE_TX_ID
    ,'**' ZIP_FILE_NAME
    ,0 TR_FILE_ID
    ,0 RET_FILE_ID
    ,1 IMAGE_STATUS
    ,NULL AXLE_COUNT    -- in ETL
    ,'**' IMAGE_TAKEN_REASON
    ,'**' PROCESS_FLAG
    ,2 IMP_TYPE
    ,EXT_PLAZA_ID PLAZA_ID
    ,'MAN' MATCH_TYPE
    ,0 FP_CONF
    ,'C' COLOR_BW

--  LPR_RESULTS.JURISDICTION
--  JOIN VPS_EVENT_ID of EVENT_ROV to VIOLATION_EVENT_ID of LPR_RESULTS
    ,NULL OCR_PLATE_COUNTRY   -- in ETL
    
    ,0 DMV_PLATE_TYPE
    ,0 IMAGE_TX_ID
    ,EXT_PLAZA_ID REDUCED_PLATE
    ,NULL MANUAL_PLATE_NUMBER  -- in ETL IF IMAGE_VERIFIED = 1 then PLATE ELSE '**'
    ,NULL MANUAL_PLATE_STATE   -- in ETL IF IMAGE_VERIFIED = 1 then JURISDICTION ELSE '**'
    ,0 MANUAL_PLATE_TYPE
    ,NULL MANUAL_PLATE_COUNTRY -- in ETL
    
-- DERIVE - EMPCODE FROM LEGACY_EMP_CODE FROM KS_USER USING USERNAME JOIN
    ,0 IMAGE_RVW_CLERK_ID      -- in ETL VIOLATION_EVENTS ?   
    ,0 REVIEWED_VEHICLE_TYPE
    ,to_date('01-01-1900','MM-DD-YYYY') IMAGE_REVIEW_TIMESTAMP  -- in ETL VIOLATION_EVENTS ?
    ,to_date('01-01-1900','MM-DD-YYYY') REVIEWED_DATE -- Required
    ,0 IMAGE_BATCH_ID
    ,0 IMAGE_BATCH_SEQ_NUMBER
    ,0 DEVICE_NO  -- Default 
    ,AVC_CLASS ACTUAL_AXLES
    ,AVC_CLASS ACTUAL_CLASS
    ,0 READ_AVI_CLASS
    ,0 REVIEWED_CLASS
    ,0 POSTCLASS_AXLES
    ,0 FP_STATUS
    ,0 OCR_THRESHOLD -- in ETL ?? PLAZAS - HIGH_LPR_CONF  default
    ,0 FP_THRESHOLD
    ,0 OCR_RUN_MODE
    ,EXT_DATE_TIME OCR_START_TIMESTAMP
    ,EXT_DATE_TIME OCR_END_TIMESTAMP
    ,1 IMG_FILE_INDEX
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_LANE_TXN lt
where txn_id = (select PA_LANE_TXN_ID
                from   KS_LEDGER kl2
                WHERE kl2.PA_LANE_TXN_ID = lt.txn_id
                AND   kl2.ACCT_NUM >= p_begin_acct_num AND   kl2.ACCT_NUM <= p_end_acct_num
                and   rownum<=1
                )
--and TRANSP_ID like '%2010'  --WHERE TRANSPONDER_ID ENDS WITH '2010'  
;

v_ks_ledger_id    KS_LEDGER.ID%TYPE:= 0;
v_acct_num        KS_LEDGER.ACCT_NUM%TYPE:= 0;
va_rec            VB_ACTIVITY%ROWTYPE;

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

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VIOL_IMAGE_TX_INFO_tab
    LIMIT P_ARRAY_SIZE;
    
    /*ETL SECTION BEGIN*/
    
    FOR i IN 1 .. DM_VIOL_IMAGE_TX_INFO_tab.COUNT LOOP
      begin
        select  ID
               ,ACCT_NUM
        into    v_ks_ledger_id
               ,v_acct_num    -- DM_VIOL_IMAGE_TX_INFO_tab(i).ETC_ACCOUNT_ID
        from    KS_LEDGER -- k1
        where   PA_LANE_TXN_ID=DM_VIOL_IMAGE_TX_INFO_tab(i).LANE_TX_ID
--        and     rownum<=1
        and     POSTED_DATE = (select max(POSTED_DATE) 
                              from    KS_LEDGER -- k2
                              where   PA_LANE_TXN_ID=DM_VIOL_IMAGE_TX_INFO_tab(i).LANE_TX_ID)
--                              where k2.id = k1.id)
        ;
      exception 
        when others then null;
--        DM_VIOL_IMAGE_TX_INFO_tab(i).TXN_DISPUTED := 0;
      end;
      
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := v_acct_num; -- DM_VIOL_IMAGE_TX_INFO_tab(i).ETC_ACCOUNT_ID;
      end if;

---- JOIN WITH JOIN TO TXN_ID OF PA_LANE_TXN RETURN HOST TO UFM_TOKEN AND 
---- JOIN UFM_TOKEN TO UFM_ID FOR EVENT_ROV       
--   JOIN VPS_EVENT_ID of EVENT_ROV to VIOLATION_EVENT_ID of LPR_RESULTS
      begin
        select 'CON'  -- ?
              ,nvl(JURISDICTION, '**')
              ,PLATE
              ,decode(IMAGE_VERIFIED, 1,0, 1)
              ,AXLE_COUNT
-- IF IMAGE_VERIFIED = 1 then PLATE ELSE '**'
              ,decode(IMAGE_VERIFIED, 1,PLATE, '**')
-- IF IMAGE_VERIFIED = 1 then JURISDICTION ELSE '**'
              ,decode(IMAGE_VERIFIED, 1,JURISDICTION, '**')  
        into  DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_CONF
              ,DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_PLATE_STATE
              ,DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_PLATE_NUMBER
              ,DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_STATUS
              ,DM_VIOL_IMAGE_TX_INFO_tab(i).AXLE_COUNT
              ,DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_NUMBER
              ,DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_STATE
        from  EVENT_ROV er, 
              UFM_LANE_TXN_INFO ult
        where er.UFM_ID = ult.HOST_UFM_TOKEN 
--        and   er.VPS_EVENT_ID
        and   ult.TXN_ID = DM_VIOL_IMAGE_TX_INFO_tab(i).LANE_TX_ID   
        and   rownum<=1
        ;
      exception
        when others then null;
        DM_VIOL_IMAGE_TX_INFO_tab(i).DEVICE_NO := '***';
      end;

-- LOOKUP PA_STATE_CODE. DM_ADDRESS_INFO and assign country correctly. 
-- visit DM_VEHICLE_INFO and DM_ACCT_INFO
      begin
        select COUNTRY into DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_COUNTRY 
        from  COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_STATE;
      exception
        when others then null;
        DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_COUNTRY:='USA';
        DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_COUNTRY:='**'; -- ??
      end;

      begin
        select substr(trim(PLAZA_NAME),1,15) -- Target is 15 chars
              ,nvl(FACCODE_FACILITY_CODE,'UNDEFINED')
        into  DM_VIOL_IMAGE_TX_INFO_tab(i).LOCATION
              ,DM_VIOL_IMAGE_TX_INFO_tab(i).FACILITY_ID
        from  PA_PLAZA
        where PLAZA_ID = DM_VIOL_IMAGE_TX_INFO_tab(i).PLAZA_ID
        ;
      exception 
        when others then null;
          DM_VIOL_IMAGE_TX_INFO_tab(i).LOCATION := 'UNDEFINED';
          DM_VIOL_IMAGE_TX_INFO_tab(i).FACILITY_ID := 'UNDEFINED';        
      end;  


      if DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_CONF is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_CONF := 'UND';
      end if;
      if DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_PLATE_COUNTRY is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_PLATE_COUNTRY := 'UND';
      end if;
      if DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_PLATE_STATE is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_PLATE_STATE := '0';
      end if;  
      if DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_PLATE_NUMBER is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_PLATE_NUMBER := 'UNDEFINED';
      end if;  
      if DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_STATUS is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_STATUS := 0;
      end if;  
      if DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_THRESHOLD is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).OCR_THRESHOLD := 0;
      end if;  
      if DM_VIOL_IMAGE_TX_INFO_tab(i).AXLE_COUNT is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).AXLE_COUNT := 0;
      end if;  
      if DM_VIOL_IMAGE_TX_INFO_tab(i).IMAGE_RVW_CLERK_ID is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).IMAGE_RVW_CLERK_ID := 0;
      end if;  
      if DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_NUMBER is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_NUMBER := 'UNDEFINED';
      end if;  
      if DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_STATE is null then
         DM_VIOL_IMAGE_TX_INFO_tab(i).MANUAL_PLATE_STATE := '0';
      end if;  


      v_trac_etl_rec.track_last_val := v_acct_num; -- DM_VIOL_IMAGE_TX_INFO_tab(i).ETC_ACCOUNT_ID;
    END LOOP;
    /*  ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_VIOL_IMAGE_TX_INFO_tab.first .. DM_VIOL_IMAGE_TX_INFO_tab.last
           INSERT INTO DM_VIOL_IMAGE_TX_INFO VALUES DM_VIOL_IMAGE_TX_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    v_trac_etl_rec.end_val := v_trac_etl_rec.track_last_val;
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

grant execute on DM_VIOL_IMAGE_TX_INFO_PROC to public;
commit;


