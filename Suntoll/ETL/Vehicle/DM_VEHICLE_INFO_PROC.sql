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

--VEHICLE_CLASS	NUMBER
--AXLE_COUNT	NUMBER(22)
--YEAR	NUMBER(4)
--VEHICLE_SEQUENCE	NUMBER(8)

CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    pav.ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,DECODE(pav.VEH_LIC_NUM, 'O', '0', pav.VEH_LIC_NUM) PLATE_NUMBER  -- PATRON.EVENT_LOOKUP_ROV
    ,pav.STATE_STATE_CODE_ABBR PLATE_STATE
    ,NULL PLATE_COUNTRY
    ,nvl(pav.VEH_LIC_TYPE,'UNDEFINED') PLATE_TYPE    -- Prefix and Suffix for the Country and State.  
    ,'REGULAR' VEHICLE_TYPE     
    ,SUBSCRIPTION_START_DATE EFFECTIVE_START_DATE
    ,SUBSCRIPTION_END_DATE EFFECTIVE_END_DATE
    ,nvl(pav.VEH_MODEL_YR, 9999) YEAR
    ,nvl(trim(pav.VEH_MAKE), 'OTHER') MAKE
    ,nvl(trim(pav.VEH_MODEL), 'OTHER') MODEL
    ,trim(pav.VEH_COLOR) COLOUR
    ,srs.REG_STOP_SENT_ON REG_STOP_START_DATE  -- ST_REG_STOP
    ,srs.REG_STOP_REMOVAL_SENT_ON REG_STOP_END_DATE -- ST_REG_STOP
    ,NULL METAL_OXIDE_WIND_SHIELD
    ,NULL DMV_RETURN_DATE  -- suntoll    --MAX(RESPONSE_DATE)Event lookup ROV
    ,0 VEHICLE_CLASS  -- Required number
    ,NULL AXLE_COUNT
    ,NULL IS_DUAL_TIRE
    ,pav.RENTAL_CAR_FLAG IS_RENTAL
    ,NULL RENTAL_COMPANY
    ,NULL RENTAL_COMPANY_PHONE
    ,pav.SUBSCRIPTION_START_DATE CREATED
    ,pav.EMP_EMP_CODE CREATED_BY
    ,pav.LAST_MODIFIED_DATE LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY   --	DEFAULT	SUNPASS_CSC_ID		LAST_UPD_BY	SUNPASS_CSC_ID (Default)
    ,nvl(pav.VEHICLE_SEQ,1) VEHICLE_SEQUENCE
    ,'SUNTOLL' SOURCE_SYSTEM
    ,ev.PLATE_ISSUE_DATE PLATE_ISSUE_DATE    -- DERIVE  PLATE_ISSUE_DATE
    ,ev.REG_EXP_DATE PLATE_EXPIRY_DATE   -- DERIVE  REG_EXP_DATE
    ,ev.DECAL_ISSUE_DATE PLATE_RENEWAL_DATE  -- DERIVE  DECAL_ISSUE_DATE
    ,ev.VIN ALT_VEHICLE_ID      -- DERIVE  VIN
    ,'UNDEFINED' SRC_LIC_VEH_NUM
FROM PA_ACCT_VEHICLE pav
    ,ST_REG_STOP srs
    ,EVENT_VEHICLE ev
WHERE pav.VEH_LIC_NUM = srs.VEH_LIC_NUM (+)
AND   pav.VEH_LIC_NUM = ev.PLATE (+)
--AND   pav.ACCT_ACCT_NUM >= p_begin_acct_num AND   pav.ACCT_ACCT_NUM <= p_end_acct_num
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

  OPEN C1;  -- (v_trac_rec.begin_acct,v_end_acct,end_acct);  
--  v_trac_etl_rec.begin_val := v_trac_rec.begin_acct;
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_VEHICLE_INFO_tab
    LIMIT P_ARRAY_SIZE;
    -- PATRON.EVENT_LOOKUP_ROV

--ETL SECTION BEGIN mapping
    FOR i IN 1 .. DM_VEHICLE_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_VEHICLE_INFO_tab(i).ACCOUNT_NUMBER;
      end if;
     
      select max(elr.RESPONSE_DATE) into  DM_VEHICLE_INFO_tab(i).DMV_RETURN_DATE
      from EVENT_LOOKUP_ROV elr
      where elr.PLATE = DM_VEHICLE_INFO_tab(i).PLATE_NUMBER
      ;
      v_trac_etl_rec.track_last_val := DM_VEHICLE_INFO_tab(i).ACCOUNT_NUMBER;
      v_trac_etl_rec.end_val := DM_VEHICLE_INFO_tab(i).ACCOUNT_NUMBER;
      
      begin
        select COUNTRY into DM_VEHICLE_INFO_tab(i).PLATE_COUNTRY from COUNTRY_STATE_LOOKUP 
        where STATE_ABBR = DM_VEHICLE_INFO_tab(i).PLATE_STATE
        and rownum<=1;
      exception
        when others then null;
        DM_VEHICLE_INFO_tab(i).PLATE_COUNTRY:='USA';
      end;
      

    END LOOP;
--      ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_VEHICLE_INFO_tab.first .. DM_VEHICLE_INFO_tab.last
           INSERT INTO DM_VEHICLE_INFO VALUES DM_VEHICLE_INFO_tab(i);
                       
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

grant execute on DM_VEHICLE_INFO_PROC to public;
commit;

