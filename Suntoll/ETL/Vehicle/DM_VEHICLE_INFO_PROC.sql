/********************************************************
*
* Name: DM_VEHICLE_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_VEHICLE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/20/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_VEHICLE_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_VEHICLE_INFO_TYP IS TABLE OF DM_VEHICLE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_VEHICLE_INFO_tab DM_VEHICLE_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;

--  Required -----------------------
--ACCOUNT_NUMBER	VARCHAR2(30)
--PLATE_NUMBER	VARCHAR2(10)
--PLATE_STATE	VARCHAR2(2)
--PLATE_COUNTRY	VARCHAR2(4)
--PLATE_TYPE	VARCHAR2(22)
--YEAR	NUMBER(4,0)
--MAKE	VARCHAR2(50)
--VEHICLE_CLASS	NUMBER
--CREATED	DATE
--LAST_UPD	DATE
--VEHICLE_SEQUENCE	NUMBER(8,0)

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,DECODE(VEH_LIC_NUM, 'O', '0', VEH_LIC_NUM) PLATE_NUMBER  -- EVENT_LOOKUP_ROV In ETL
    ,STATE_STATE_CODE_ABBR PLATE_STATE
    ,NULL PLATE_COUNTRY   -- in ETL
    ,nvl(VEH_LIC_TYPE||STATE_STATE_CODE_ABBR,'0') PLATE_TYPE  -- Prefix and Suffix for the Country and State.  
    ,'REGULAR' VEHICLE_TYPE     
    ,SUBSCRIPTION_START_DATE EFFECTIVE_START_DATE
    ,SUBSCRIPTION_END_DATE EFFECTIVE_END_DATE
--    ,9999 YEAR
    ,CASE when VEH_MODEL_YR is NULL then '9999'          
           when LENGTH(TRIM(TRANSLATE(VEH_MODEL_YR, ' +-.0123456789', ' '))) is NOT null then '9990' -- ?
           when VEH_MODEL_YR = '0' then '9000' -- ?
           when length(VEH_MODEL_YR) = 4 then VEH_MODEL_YR
           when substr(VEH_MODEL_YR,1,1) in ('0','1') THEN '20'||VEH_MODEL_YR
           else '19'||VEH_MODEL_YR
     END YEAR
    ,nvl(trim(VEH_MAKE), 'OTHER') MAKE
    ,nvl(trim(VEH_MODEL), 'OTHER') MODEL
    ,trim(VEH_COLOR) COLOUR
    ,NULL REG_STOP_START_DATE   -- ST_REG_STOP IN ETL
    ,NULL REG_STOP_END_DATE     -- ST_REG_STOP in ETL
    ,NULL METAL_OXIDE_WIND_SHIELD
    ,NULL DMV_RETURN_DATE   -- In ETL  --MAX(RESPONSE_DATE)Event lookup ROV
    ,0 VEHICLE_CLASS  -- Required number
    ,NULL AXLE_COUNT
    ,NULL IS_DUAL_TIRE
    ,RENTAL_CAR_FLAG IS_RENTAL
    ,NULL RENTAL_COMPANY
    ,NULL RENTAL_COMPANY_PHONE
    ,SUBSCRIPTION_START_DATE CREATED
    ,EMP_EMP_CODE CREATED_BY
    ,LAST_MODIFIED_DATE LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY   --	DEFAULT	SUNPASS_CSC_ID		LAST_UPD_BY	SUNPASS_CSC_ID (Default)
    ,nvl(VEHICLE_SEQ,1) VEHICLE_SEQUENCE
    ,'SUNTOLL' SOURCE_SYSTEM
    ,NULL PLATE_ISSUE_DATE    -- In ETL - DERIVE  PLATE_ISSUE_DATE
    ,NULL PLATE_EXPIRY_DATE   -- In ETL - DERIVE  REG_EXP_DATE
    ,NULL PLATE_RENEWAL_DATE  -- In ETL - DERIVE  DECAL_ISSUE_DATE
    ,NULL ALT_VEHICLE_ID      -- In ETL - DERIVE  VIN
    ,'UNDEFINED' SRC_LIC_VEH_NUM
FROM PA_ACCT_VEHICLE
WHERE ACCT_ACCT_NUM >= p_begin_acct_num AND   ACCT_ACCT_NUM <= p_end_acct_num
and   ACCT_ACCT_NUM >0
; -- FTE source

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
    FETCH C1 BULK COLLECT INTO DM_VEHICLE_INFO_tab
    LIMIT P_ARRAY_SIZE;

--ETL SECTION BEGIN mapping
    FOR i IN 1 .. DM_VEHICLE_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_VEHICLE_INFO_tab(i).ACCOUNT_NUMBER;
      end if;

      begin
        select COUNTRY 
        into  DM_VEHICLE_INFO_tab(i).PLATE_COUNTRY 
        from  COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_VEHICLE_INFO_tab(i).PLATE_STATE
        ;
      exception
        when others then null;
          DM_VEHICLE_INFO_tab(i).PLATE_COUNTRY:='0';
      end;
      DM_VEHICLE_INFO_tab(i).PLATE_TYPE := DM_VEHICLE_INFO_tab(i).PLATE_COUNTRY||DM_VEHICLE_INFO_tab(i).PLATE_TYPE;
     
      begin
        select max(RESPONSE_DATE) 
        into  DM_VEHICLE_INFO_tab(i).DMV_RETURN_DATE
        from  EVENT_LOOKUP_ROV
        where PLATE = DM_VEHICLE_INFO_tab(i).PLATE_NUMBER
        ;
      exception
        when others then null;
        DM_VEHICLE_INFO_tab(i).DMV_RETURN_DATE := NULL;
      end;

      begin
        select REG_STOP_SENT_ON
              ,REG_STOP_REMOVAL_SENT_ON
        into  DM_VEHICLE_INFO_tab(i).REG_STOP_START_DATE
              ,DM_VEHICLE_INFO_tab(i).REG_STOP_END_DATE
        from  ST_REG_STOP
        where VEH_LIC_NUM = DM_VEHICLE_INFO_tab(i).PLATE_NUMBER
        ;
      exception
        when no_data_found THEN
          DM_VEHICLE_INFO_tab(i).REG_STOP_START_DATE:=NULL;
          DM_VEHICLE_INFO_tab(i).REG_STOP_END_DATE:=NULL;
        when others then null;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('ST_REG_STOP ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;
            
--    ,ev.PLATE_ISSUE_DATE PLATE_ISSUE_DATE     -- DERIVE  PLATE_ISSUE_DATE
--    ,ev.REG_EXP_DATE PLATE_EXPIRY_DATE        -- DERIVE  REG_EXP_DATE
--    ,ev.DECAL_ISSUE_DATE PLATE_RENEWAL_DATE   -- DERIVE  DECAL_ISSUE_DATE
--    ,ev.VIN ALT_VEHICLE_ID                    -- DERIVE  VIN
      begin
        select PLATE_ISSUE_DATE
              ,REG_EXP_DATE
              ,DECAL_ISSUE_DATE
              ,VIN
        into  DM_VEHICLE_INFO_tab(i).PLATE_ISSUE_DATE
              ,DM_VEHICLE_INFO_tab(i).PLATE_EXPIRY_DATE
              ,DM_VEHICLE_INFO_tab(i).PLATE_RENEWAL_DATE
              ,DM_VEHICLE_INFO_tab(i).ALT_VEHICLE_ID
        from  EVENT_VEHICLE
        where PLATE = DM_VEHICLE_INFO_tab(i).PLATE_NUMBER
        and   start_date = (select max(start_date) 
                          from EVENT_VEHICLE
                          where PLATE = DM_VEHICLE_INFO_tab(i).PLATE_NUMBER)
        ;
      exception
        when no_data_found THEN NULL;
          DM_VEHICLE_INFO_tab(i).PLATE_ISSUE_DATE:=NULL;
          DM_VEHICLE_INFO_tab(i).PLATE_EXPIRY_DATE:=NULL;
          DM_VEHICLE_INFO_tab(i).PLATE_RENEWAL_DATE:=NULL;
          DM_VEHICLE_INFO_tab(i).ALT_VEHICLE_ID:=NULL;
        when others then null;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('EVENT_VEHICLE ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;
      
      v_trac_etl_rec.track_last_val := DM_VEHICLE_INFO_tab(i).ACCOUNT_NUMBER;

    END LOOP;
--      ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_VEHICLE_INFO_tab.first .. DM_VEHICLE_INFO_tab.last
           INSERT INTO DM_VEHICLE_INFO VALUES DM_VEHICLE_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    v_trac_etl_rec.end_val := v_trac_etl_rec.track_last_val;
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

grant execute on DM_VEHICLE_INFO_PROC to public;
commit;

