/********************************************************
*
* Name: DM_VEHICLE_HST_INFO_PROC
* Created by: RH, 6/2/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_VEHICLE_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- 6/27/2016 RH Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_VEHICLE_HST_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_VEHICLE_HST_INFO_TYP IS TABLE OF DM_VEHICLE_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_VEHICLE_HST_INFO_tab DM_VEHICLE_HST_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,DECODE(VEH_LIC_NUM, 'O', '0', VEH_LIC_NUM) PLATE_NUMBER  -- PATRON.EVENT_LOOKUP_ROV
    ,STATE_STATE_CODE_ABBR PLATE_STATE
    ,NULL PLATE_COUNTRY   -- in ETL
    ,nvl(VEH_LIC_TYPE||STATE_STATE_CODE_ABBR,'0') PLATE_TYPE  -- Prefix and Suffix for the Country and State.  
    ,'REGULAR' VEHICLE_TYPE     
    ,SUBSCRIPTION_START_DATE EFFECTIVE_START_DATE
    ,SUBSCRIPTION_END_DATE EFFECTIVE_END_DATE
    ,CASE when VEH_MODEL_YR is NULL then '9999'
           when VEH_MODEL_YR = '0' then '9000' -- ?
           when length(VEH_MODEL_YR) = 4 then VEH_MODEL_YR
           when substr(VEH_MODEL_YR,1,1) in ('0','1') THEN '20'||VEH_MODEL_YR
           else '19'||VEH_MODEL_YR
     END YEAR
    ,nvl(trim(VEH_MAKE), 'OTHER') MAKE
    ,nvl(trim(VEH_MODEL), 'OTHER') MODEL
    ,trim(VEH_COLOR) COLOUR
    ,NULL REG_STOP_START_DATE  -- ST_REG_STOP  in ETL
    ,NULL REG_STOP_END_DATE -- ST_REG_STOP
    ,NULL METAL_OXIDE_WIND_SHIELD
    ,NULL DMV_RETURN_DATE -- suntoll    --MAX(RESPONSE_DATE)Event lookup ROV
    ,0 VEHICLE_CLASS  -- Required number
    ,NULL AXLE_COUNT
    ,NULL IS_DUAL_TIRE
    ,NULL IS_RENTAL
    ,RENTAL_CAR_FLAG IS_RENTAL
    ,NULL RENTAL_COMPANY_PHONE
    ,SUBSCRIPTION_START_DATE CREATED
    ,EMP_EMP_CODE CREATED_BY
    ,LAST_MODIFIED_DATE LAST_UPD
    ,NULL LAST_UPD_BY
    ,VEHICLE_SEQ VEHICLE_SEQUENCE
    ,'SUNTOLL' SOURCE_SYSTEM
    ,NULL ACTIVITY_ID
    ,NULL REBILL_PAY_TYPE_INTEGRATION_ID
    ,NULL EMP_NUM
    ,NULL IS_MVA
    ,NULL X_LICENSE_STATUS
    ,NULL X_PLATE_VIN_NO
    ,NULL STATUS
    ,NULL X_ALTERNATEID
    ,NULL X_OWNER
    ,NULL X_WEB_VEHICLE_TYPE
    ,NULL VERSION
FROM PA_ACCT_VEHICLE pav
WHERE ACCT_ACCT_NUM >= p_begin_acct_num AND   ACCT_ACCT_NUM <= p_end_acct_num
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
--  v_trac_etl_rec.begin_val := v_trac_rec.begin_acct;
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VEHICLE_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;

--ETL SECTION BEGIN mapping
    FOR i IN 1 .. DM_VEHICLE_HST_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_VEHICLE_HST_INFO_tab(i).ACCOUNT_NUMBER;
      end if;

      begin
        select COUNTRY 
        into  DM_VEHICLE_HST_INFO_tab(i).PLATE_COUNTRY 
        from  COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_VEHICLE_HST_INFO_tab(i).PLATE_STATE
        ;
      exception
        when others then null;
          DM_VEHICLE_HST_INFO_tab(i).PLATE_COUNTRY:='0';
      end;
      DM_VEHICLE_HST_INFO_tab(i).PLATE_TYPE := DM_VEHICLE_HST_INFO_tab(i).PLATE_COUNTRY||DM_VEHICLE_HST_INFO_tab(i).PLATE_TYPE;
      
      select max(RESPONSE_DATE) 
      into  DM_VEHICLE_HST_INFO_tab(i).DMV_RETURN_DATE
      from  EVENT_LOOKUP_ROV
      where PLATE = DM_VEHICLE_HST_INFO_tab(i).PLATE_NUMBER
      ;

      begin
        select REG_STOP_SENT_ON
              ,REG_STOP_REMOVAL_SENT_ON
        into  DM_VEHICLE_HST_INFO_tab(i).REG_STOP_START_DATE
              ,DM_VEHICLE_HST_INFO_tab(i).REG_STOP_END_DATE
        from  ST_REG_STOP
        where VEH_LIC_NUM = DM_VEHICLE_HST_INFO_tab(i).PLATE_NUMBER
        ;
      exception
        when no_data_found THEN
          DM_VEHICLE_HST_INFO_tab(i).REG_STOP_START_DATE:=NULL;
          DM_VEHICLE_HST_INFO_tab(i).REG_STOP_END_DATE:=NULL;
        when others then null;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('ST_REG_STOP ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;

      v_trac_etl_rec.track_last_val := DM_VEHICLE_HST_INFO_tab(i).ACCOUNT_NUMBER;

    END LOOP;
--      ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_VEHICLE_HST_INFO_tab.first .. DM_VEHICLE_HST_INFO_tab.last
           INSERT INTO DM_VEHICLE_HST_INFO VALUES DM_VEHICLE_HST_INFO_tab(i);
                       
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

grant execute on DM_PAYMT_HST_INFO_PROC to public;
commit;

