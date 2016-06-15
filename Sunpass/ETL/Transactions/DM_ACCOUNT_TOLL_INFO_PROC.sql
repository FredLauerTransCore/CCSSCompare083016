/********************************************************
*
* Name: DM_ACCOUNT_TOLL_INFO_PROC
* Created by: DT, 5/3/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCOUNT_TOLL_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_ACCOUNT_TOLL_INFO_PROC IS

TYPE DM_ACCOUNT_TOLL_INFO_TYP IS TABLE OF DM_ACCOUNT_TOLL_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_TOLL_INFO_tab DM_ACCOUNT_TOLL_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;
t_STATE_ID_CODE varchar2(2);
t_uo_code varchar2(2);

CURSOR C1 IS SELECT 
    NULL TX_EXTERN_REF_NO
    ,EXT_MSG_SEQ_NUM TX_SEQ_NUMBER
    ,TOUR_TOUR_SEQ EXTERN_FILE_ID
    ,EXT_LANE_ID LANE_ID
    ,EXT_DATE_TIME TX_TIMESTAMP
    ,decode(TRANSP_ID,null,0,1) TX_MOD_SEQ
    ,NULL TX_TYPE_IND
    ,NULL TX_SUBTYPE_IND
    ,decode(substr(ext_PLAZA_ID,1,3),'004','C','B') TOLL_SYSTEM_TYPE
    ,EXT_LANE_TYPE_CODE LANE_MODE
    ,'1' LANE_TYPE
    ,'0' LANE_STATE
    ,'0' LANE_HEALTH
    ,NULL PLAZA_AGENCY_ID
    ,EXT_PLAZA_ID PLAZA_ID
    ,'0' COLLECTOR_ID
    ,'0' TOUR_SEGMENT_ID
    ,'0' ENTRY_DATA_SOURCE
    ,ENT_LANE_ID ENTRY_LANE_ID
    ,ENT_PLAZA_ID ENTRY_PLAZA_ID
    ,ENT_DATE_TIME ENTRY_TIMESTAMP
    ,TXN_ENTRY_NUMBER ENTRY_TX_SEQ_NUMBER
    ,'0' ENTRY_VEHICLE_SPEED
    ,'0' LANE_TX_STATUS
    ,'0' LANE_TX_TYPE
    ,'0' TOLL_REVENUE_TYPE
    ,AVC_CLASS ACTUAL_CLASS
    ,AVC_CLASS ACTUAL_AXLES
    ,'0' ACTUAL_EXTRA_AXLES
    ,'0' COLLECTOR_CLASS
    ,'0' COLLECTOR_AXLES
    ,'0' PRECLASS_CLASS
    ,'0' PRECLASS_AXLES
    ,'0' POSTCLASS_CLASS
    ,'0' POSTCLASS_AXLES
    ,'0' FORWARD_AXLES
    ,'0' REVERSE_AXLES
    ,TOLL_AMT_FULL FULL_FARE_AMOUNT
    ,TOLL_AMT_CHARGED DISCOUNTED_AMOUNT
    ,TOLL_AMT_COLLECTED COLLECTED_AMOUNT
    ,'0' UNREALIZED_AMOUNT
    ,'N' IS_DISCOUNTABLE
    ,'N' IS_MEDIAN_FARE
    ,'N' IS_PEAK
    ,'0' PRICE_SCHEDULE_ID
    ,'0' VEHICLE_SPEED
    ,'0' RECEIPT_ISSUED
    ,TRANSP_ID DEVICE_NO
    ,NULL ACCOUNT_TYPE
    ,TRANSP_CLASS DEVICE_CODED_CLASS
    ,'0' DEVICE_AGENCY_CLASS
    ,'0' DEVICE_IAG_CLASS
    ,'0' DEVICE_AXLES
    ,NULL ETC_ACCOUNT_ID
    ,'FTE' ACCOUNT_AGENCY_ID
    ,TRANSP_CLASS READ_AVI_CLASS
    ,'0' READ_AVI_AXLES
    ,'N' DEVICE_PROGRAM_STATUS
    ,'N' BUFFERED_READ_FLAG
    ,MSG_INVALID LANE_DEVICE_STATUS
    ,'0' POST_DEVICE_STATUS
    ,NULL PRE_TXN_BALANCE
    ,'1' PLAN_TYPE_ID
    ,NULL ETC_TX_STATUS
    ,'F' SPEED_VIOL_FLAG
    ,decode(msg_id,'ITOL','Y','N') IMAGE_TAKEN
    ,NULL PLATE_COUNTRY
    ,NULL PLATE_STATE
    ,VEH_LIC_NUM PLATE_NUMBER
    ,trunc(EXT_DATE_TIME) REVENUE_DATE
    ,trunc(TXN_PROCESS_DATE) POSTED_DATE
    ,'0' ATP_FILE_ID
    ,TXN_PROCESS_DATE UPDATE_TS
    ,'N' IS_REVERSED
    ,'0' CORR_REASON_ID
    ,trunc(TXN_PROCESS_DATE) RECON_DATE
    ,'0' RECON_STATUS_IND
    ,'0' RECON_SUB_CODE_IND
    ,NULL EXTERN_FILE_DATE
    ,'0' MILEAGE
    ,'0' DEVICE_READ_COUNT
    ,'0' DEVICE_WRITE_COUNT
    ,'0' ENTRY_DEVICE_READ_COUNT
    ,'0' ENTRY_DEVICE_WRITE_COUNT
    ,'000000000000' DEPOSIT_ID
    ,trunc(EXT_DATE_TIME) TX_DATE
    ,NULL LOCATION
    ,NULL FACILITY_ID
    ,'0' FARE_TBL_ID
    ,'0' TRAN_CODE
    ,'SUNPASS' SOURCE_SYSTEM
    ,TXN_ID LANE_TX_ID
    ,TRANSP_INTERNAL_NUM DEVICE_INTERNAL_NUMBER
FROM PA_LANE_TXN
WHERE TRANSP_ID like  '%0110' or 
    TRANSP_ID like '%0210' AND ( EXT_PLAZA_ID NOT LIKE '200%' AND EXT_PLAZA_ID NOT LIKE '008%');

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_TOLL_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */



    FOR i in 1 .. DM_ACCOUNT_TOLL_INFO_tab.count loop

    /* get UFM_LANE_TXN_INFO.HOST_UFM_TOKEN for TX_EXTERN_REF_NO */
   /* get UFM_LANE_TXN_INFO.HOST_UFM_TOKEN for TX_EXTERN_REF_NO */
    begin
      select HOST_UFM_TOKEN into DM_ACCOUNT_TOLL_INFO_tab(i).TX_EXTERN_REF_NO from UFM_LANE_TXN_INFO 
      where TXN_ID=DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TX_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).TX_EXTERN_REF_NO:=null;
    end;

begin
select uo_code, state_id_code into t_uo_code, t_STATE_ID_CODE from pa_lane_txn where DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TX_ID=txn_id;
EXCEPTION
WHEN OTHERS THEN
  NULL;
end;	
    /* get PA_PLAZA.AGENCY_ID for PLAZA_AGENCY_ID */
BEGIN
  SELECT agency_id
  INTO DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_AGENCY_ID
  FROM PA_PLAZA b,
    ST_INTEROP_AGENCIES c
  WHERE DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID=b.PLAZA_ID
  AND t_uo_code       =c.AUTHORITY_CODE
  AND rownum                                   <=1;
EXCEPTION
WHEN OTHERS THEN
  NULL;
  DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_AGENCY_ID:=NULL;
END;

    /* get PA_STATE_CODE.STATE_CODE_ABB for PLATE_STATE */
    begin
      select STATE_CODE_ABBR into DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_STATE from PA_STATE_CODE 
      where STATE_CODE_NUM=t_state_id_code
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_STATE:=null;
    end;

    /* get PA_TOUR.COLLECTION_DATE for EXTERN_FILE_DATE */
    begin
      select COLLECTION_DATE into DM_ACCOUNT_TOLL_INFO_tab(i).EXTERN_FILE_DATE from PA_TOUR 
      where TOUR_SEQ=DM_ACCOUNT_TOLL_INFO_tab(i).EXTERN_FILE_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).EXTERN_FILE_DATE:=null;
    end;

    /* get PA_PLAZA.PLAZA_NAME for LOCATION */
    begin
      select PLAZA_NAME into DM_ACCOUNT_TOLL_INFO_tab(i).LOCATION from PA_PLAZA 
      where PLAZA_ID=DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).LOCATION:=null;
    end;

    /* get PA_PLAZA.FACCODE_FACILITY_CODE for FACILITY_ID */
    begin
      select FACCODE_FACILITY_CODE into DM_ACCOUNT_TOLL_INFO_tab(i).FACILITY_ID from PA_PLAZA 
      where PLAZA_ID=DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).FACILITY_ID:=null;
    end;

    /* get PA_PLAZA.decode(FTE_PLAZA,'N','I',decode(MSG_ID,'ITOL','V','E')) for TX_TYPE_IND */
    begin
      select decode(FTE_PLAZA,'N','I') into DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND from PA_PLAZA 
      where PLAZA_ID=DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND:=null;
    end;

    end loop;

   /* this is for the TX_SUBTYPE_IND
   IF TX_TYPE_IND = 'E' THEN 'Z' 
   ELSE IF TX_TYPE_IND = 'V' THEN 
     IF  TRANSP_ID ENDS WITH '0210' THEN 'I' 
         ELSE 'T' ELSE IF TX_TYPE_IND = 'I' THEN 
         IF MSG_ID = 'TTOL' THEN 'C' 
         ELSE 'I'

   */

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_TOLL_INFO_tab.first .. DM_ACCOUNT_TOLL_INFO_tab.last
           INSERT INTO DM_ACCOUNT_TOLL_INFO VALUES DM_ACCOUNT_TOLL_INFO_tab(i);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;

  COMMIT;

  CLOSE C1;

  COMMIT;

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
END;
/
SHOW ERRORS


