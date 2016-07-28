/********************************************************
*
* Name: DM_DMV_INQUIRY_INFO_PROC
* Created by: RH, 5/19/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_DMV_INQUIRY_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

--EXTRACT RULE - 
--select * 
--  Join OAVA_LINK_ID of PA_LANE_TXN to ID of EVENT_OWNER_ADDR_VEHICLE_ACCT AND 
--  OWNER_ID    of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_OWNER AND 
--  ADDRESS_ID  of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_ADDRESS AND 
--  VEHICLE_ID  of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_VEHICLE

CREATE OR REPLACE PROCEDURE DM_DMV_INQUIRY_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_DMV_INQUIRY_INFO_TYP IS TABLE OF DM_DMV_INQUIRY_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_DMV_INQUIRY_INFO_tab DM_DMV_INQUIRY_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

-- (PK) X_ETC_ACCOUNT_ID, X_PLATE_NUM, X_STATUS 

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT  distinct
    0 MODIFICATION_NUM --number of times the account or plate informations were modified.
    ,'0' CONFLICT_ID
    ,ACCT_NUM X_ETC_X_ACCOUNT_ID
    ,'0' X_HIST_FLG
    ,ACCT_NUM X_ACCOUNT_ID  -- JOIN TO EVENT_ADDRESS TABLE TO GAIN X_ACCOUNT_ID ??
    ,'UNDEFINED' X_ADDR_LINE_1
    ,'UNDEFINED' X_ADDR_LINE_2
    ,'UNDEFINED' X_CITY
--    ,'UNDEFINED' X_CONTACT_ID
    ,ADDRESS_ID X_CONTACT_ID
    ,'UNDEFINED' X_COUNTRY

-- IF REG_STOP_SENT_ON IS NOT NULL AND REG_REMOVAL_SENT_ON IS NULL THEN  REG_STOP ELSE 'N' 
    ,'N' X_DMV_STATUS  -- in ETL
      
    ,'UNDEFINED' X_FST_NAME   -- in ETL
    ,'UNDEFINED' X_LAST_NAME  -- in ETL
    ,VEHICLE_ID X_PLATE_NUM  -- in ETL
--    ,'UNDEFINED' X_PLATE_NUM  -- in ETL
    ,'N' X_SEND_DMV  -- IF REG_STOP_SENT_ON IS NOT NULL THEN 'Y' ELSE 'N'
    ,NULL X_STATE    -- in ETL
    ,'00000' X_STATUS
    ,NULL X_TOD_ID
    ,NULL X_ZIPCODE
    ,CREATED_ON CREATED
    ,OWNER_ID CREATED_BY
    ,SYSDATE LAST_UPD
    ,'CURRENT' LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
FROM EVENT_OWNER_ADDR_VEHICLE_ACCT e1
--    ,EVENT_ADDRESS ea
--    ,EVENT_VEHICLE ev
--    ,EVENT_OWNER eo
--    ,ST_REG_STOP srs   ,PA_LANE_TXN lt
--WHERE e.OWNER_ID = eo.ID (+)
--AND e.ADDRESS_ID = ea.ID (+)
--AND e.VEHICLE_ID = ev.ID (+)
--AND e.ACCT_NUM = srs.ACCT_NUM (+)
where ACCT_NUM >= p_begin_acct_num AND  ACCT_NUM <= p_end_acct_num
and   id = (select max(id) 
            from EVENT_OWNER_ADDR_VEHICLE_ACCT e2
            where e2.ACCT_NUM = e1.ACCT_NUM
            and e2.VEHICLE_ID = e1.VEHICLE_ID
            )
; 
-- Source

-- EXTRACT RULE - 
--select * 
--Join OAVA_LINK_ID of PA_LANE_TXN to ID of EVENT_OWNER_ADDR_VEHICLE_ACCT 
--AND OWNER_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_OWNER 
--AND ADDRESS_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_ADDRESS 
--AND VEHICLE_ID of EVENT_OWNER_ADDR_VEHICLE_ACCT to ID of EVENT_VEHICLE
v_event_rec     EVENT_OWNER_ADDR_VEHICLE_ACCT%ROWTYPE;
v_msg           varchar2(200);
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
--  v_trac_etl_rec.begin_val := v_trac_rec.begin_acct;
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_DMV_INQUIRY_INFO_tab
    LIMIT P_ARRAY_SIZE;

--ETL SECTION BEGIN mapping
    FOR i IN 1 .. DM_DMV_INQUIRY_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_DMV_INQUIRY_INFO_tab(i).X_ACCOUNT_ID;
      end if;
      
--      begin
--        select distinct * into v_event_rec
--        from  EVENT_OWNER_ADDR_VEHICLE_ACCT
--        where ACCT_NUM = DM_DMV_INQUIRY_INFO_tab(i).X_ACCOUNT_ID
--        and   ID = (select max(ID) from EVENT_OWNER_ADDR_VEHICLE_ACCT
--                    where ACCT_NUM = DM_DMV_INQUIRY_INFO_tab(i).X_ACCOUNT_ID)
--        ;
--  
--      exception
--        when others then null;
--          v_trac_etl_rec.result_code := SQLCODE;
--          v_trac_etl_rec.result_msg := SQLERRM;
--          v_trac_etl_rec.proc_end_date := SYSDATE;
--          update_track_proc(v_trac_etl_rec);
--          DBMS_OUTPUT.PUT_LINE('v_event_rec ERROR CODE: '||v_trac_etl_rec.result_code);
--          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
--      end;     

      V_MSG := i||'. X_ACCOUNT_ID: '||DM_DMV_INQUIRY_INFO_tab(i).X_ACCOUNT_ID;
---- IF REG_STOP_SENT_ON IS NOT NULL AND REG_REMOVAL_SENT_ON IS NULL THEN  REG_STOP ELSE 'N' 
      begin
        select distinct 
          CASE WHEN REG_STOP_SENT_ON IS NOT NULL AND REG_STOP_REMOVAL_SENT_ON IS NULL
               THEN 'Y'  --srs.REG_STOP // Further action required per comment ??
               ELSE 'N'
          END 
              ,nvl2(REG_STOP_SENT_ON,'Y','N')
        into  DM_DMV_INQUIRY_INFO_tab(i).X_DMV_STATUS
              ,DM_DMV_INQUIRY_INFO_tab(i).X_SEND_DMV
        from  ST_REG_STOP
        where ACCT_NUM = DM_DMV_INQUIRY_INFO_tab(i).X_ACCOUNT_ID
        ;
      exception
        when no_data_found THEN NULL;
        when others then null;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('ST_REG_STOP ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;
 
--    ,nvl(ea.ADDR1,'UNDEFINED') X_ADDR_LINE_1
--    ,nvl(ea.ADDR2,'UNDEFINED') X_ADDR_LINE_2
--    ,nvl(ea.CITY,'UNDEFINED') X_CITY
--    ,decode(nvl(ea.ID,0),0,'UNDEFINED',ea.ID) X_CONTACT_ID
--    ,decode(nvl(ea.COUNTRY_CODE,0),0,'UNDEFINED',ea.COUNTRY_CODE) X_COUNTRY
--    ,ea.STATE_ABBR X_STATE    -- in ETL
--    ,ea.ZIP X_ZIPCODE
      V_MSG := V_MSG||' X_CONTACT_ID: '||DM_DMV_INQUIRY_INFO_tab(i).X_CONTACT_ID;
      begin
        select distinct ADDR1
              ,nvl(ADDR2,'UNDEFINED')
              ,nvl(CITY,'UNDEFINED')
              ,COUNTRY_CODE
              ,nvl(STATE_ABBR,'UNDEFINED')
              ,nvl(ZIP,'UNDEFINED')
        into  DM_DMV_INQUIRY_INFO_tab(i).X_ADDR_LINE_1
              ,DM_DMV_INQUIRY_INFO_tab(i).X_ADDR_LINE_2
              ,DM_DMV_INQUIRY_INFO_tab(i).X_CITY
              ,DM_DMV_INQUIRY_INFO_tab(i).X_COUNTRY
              ,DM_DMV_INQUIRY_INFO_tab(i).X_STATE
              ,DM_DMV_INQUIRY_INFO_tab(i).X_ZIPCODE
        from  EVENT_ADDRESS
        where ID = DM_DMV_INQUIRY_INFO_tab(i).X_CONTACT_ID
        ;
      exception
        when no_data_found THEN NULL;
        when others then null;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('EVENT_ADDRESS ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;     
    
      begin
        select distinct nvl(FIRST_NAME,'UNDEFINED')
              ,substr(LAST_ORG_NAME,1,50)
        into  DM_DMV_INQUIRY_INFO_tab(i).X_FST_NAME
              ,DM_DMV_INQUIRY_INFO_tab(i).X_LAST_NAME
        from  EVENT_OWNER
        where ID = DM_DMV_INQUIRY_INFO_tab(i).CREATED_BY
--        and rownum <=1
        ;
      exception
        when no_data_found THEN NULL;
          DM_DMV_INQUIRY_INFO_tab(i).X_FST_NAME:='UNDEFINED';
          DM_DMV_INQUIRY_INFO_tab(i).X_LAST_NAME:='UNDEFINED';
        when others then null;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('EVENT_OWNER ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;
      
-- ,nvl(ev.PLATE,'UNDEFINED') X_PLATE_NUM
      V_MSG := V_MSG||' X_PLATE_NUM: '||DM_DMV_INQUIRY_INFO_tab(i).X_PLATE_NUM;
      begin
        select distinct PLATE
        into  DM_DMV_INQUIRY_INFO_tab(i).X_PLATE_NUM
        from  EVENT_VEHICLE
        where ID = DM_DMV_INQUIRY_INFO_tab(i).X_PLATE_NUM -- v_event_rec.VEHICLE_ID
        ;
      exception
        when no_data_found THEN NULL;
          DM_DMV_INQUIRY_INFO_tab(i).X_PLATE_NUM:='UNDEFINED';
        when others then null;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('EVENT_VEHICLE ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;
      
      v_trac_etl_rec.track_last_val := DM_DMV_INQUIRY_INFO_tab(i).X_ACCOUNT_ID;
      v_trac_etl_rec.end_val := DM_DMV_INQUIRY_INFO_tab(i).X_ACCOUNT_ID;

    END LOOP;
--      ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_DMV_INQUIRY_INFO_tab.first .. DM_DMV_INQUIRY_INFO_tab.last
           INSERT INTO DM_DMV_INQUIRY_INFO VALUES DM_DMV_INQUIRY_INFO_tab(i);

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
     DBMS_OUTPUT.PUT_LINE('V_MSG: '||V_MSG);
     DBMS_OUTPUT.PUT_LINE('DNV ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS

grant execute on DM_DMV_INQUIRY_INFO_PROC to public;
commit;


