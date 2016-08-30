/********************************************************
*
* Name: DM_AWAY_AGENCY_TX_TNSP_PROC
* Created by: DT, 5/2/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_AWAY_AGENCY_TX_TNSP_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_AWAY_AGENCY_TX_TNSP_PROC IS

TYPE DM_AWAY_AGENCY_TX_TNSP_TYP IS TABLE OF DM_AWAY_AGENCY_TX_TNSP_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_AWAY_AGENCY_TX_TNSP_tab DM_AWAY_AGENCY_TX_TNSP_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL TX_EXTERN_REF_NO
    ,EXT_MSG_SEQ_NUM TX_SEQ_NUMBER
    ,'0' EXTERN_FILE_ID
    ,EXT_LANE_ID LANE_ID
    ,EXT_DATE_TIME TX_TIMESTAMP
    ,decode(trans_source,null,0,1) TX_MOD_SEQ
    ,'O' TX_TYPE_IND
    ,'T' TX_SUBTYPE_IND
    ,decode(substr(ext_plaza_id,1,3),'004','C','B') TOLL_SYSTEM_TYPE
    ,decode(EXT_LANE_TYPE_CODE 
  	       ,'A','100'
           ,'E','101'
           ,'M','102'
           ,'D','103'
           ,'C','104'
           ,'B','105'
           ,'N','106'
           ,'X','107'
           ,'F','108'
           ,'Y','109'
           ,'Z','110'
           ,'S','111'
           ,'G','112'
           ,'Q','113'
           ,'T','114',0)
	LANE_MODE
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
    ,'0' ACCOUNT_TYPE
    ,TRANSP_CLASS DEVICE_CODED_CLASS
    ,'0' DEVICE_AGENCY_CLASS
    ,'0' DEVICE_IAG_CLASS
    ,'0' DEVICE_AXLES
    ,'0' ETC_ACCOUNT_ID
    ,SUBSTR(TRANSP_ID,9,2) ACCOUNT_AGENCY_ID
    ,TRANSP_CLASS READ_AVI_CLASS
    ,'0' READ_AVI_AXLES
    ,'N' DEVICE_PROGRAM_STATUS
    ,'N' BUFFERED_READ_FLAG
    ,MSG_INVALID LANE_DEVICE_STATUS
    ,'0' POST_DEVICE_STATUS
    ,'0' PRE_TXN_BALANCE
    ,'1' PLAN_TYPE_ID
    ,'7' ETC_TX_STATUS
    ,'F' SPEED_VIOL_FLAG
    ,decode(msg_id,'ITOL','Y','N') IMAGE_TAKEN
    ,'..' PLATE_COUNTRY
    ,STATE_ID_CODE PLATE_STATE
    ,VEH_LIC_NUM PLATE_NUMBER
    ,trunc(EXT_DATE_TIME) REVENUE_DATE
    ,trunc(TXN_PROCESS_DATE) POSTED_DATE
    ,'0' ATP_FILE_ID
    ,TXN_PROCESS_DATE UPDATE_TS
    ,'N' IS_REVERSED
    ,'0' CORR_REASON_ID
    ,TXN_PROCESS_DATE RECON_DATE
    ,'0' RECON_STATUS_IND
    ,'0' RECON_SUB_CODE_IND
    ,to_date('01/01/1900','MM/DD/YYYY') EXTERN_FILE_DATE
    ,'0' MILEAGE
    ,'0' DEVICE_READ_COUNT
    ,'0' DEVICE_WRITE_COUNT
    ,'0' ENTRY_DEVICE_READ_COUNT
    ,'0' ENTRY_DEVICE_WRITE_COUNT
    ,'000000000000' DEPOSIT_ID
    ,trunc(EXT_DATE_TIME) TX_DATE
    ,NULL LOCATION
    ,'0' FARE_TBL_ID
    ,EXT_MSG_SEQ_NUM LANE_SEQ_NO
    ,'0' HUB_REF_ID
    ,'SUNPASS' SOURCE_SYSTEM
    ,TXN_ID LANE_TX_ID
FROM PA_LANE_TXN where (TRANSP_ID not like '%0110' and TRANSP_ID not like '%0210')
and (ext_plaza_id not like '200%' and ext_plaza_id not like  '0008%'  and ext_plaza_id not like '30001%');

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_AWAY_AGENCY_TX_TNSP_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */



    FOR i in 1 .. DM_AWAY_AGENCY_TX_TNSP_tab.count loop

    /* get PA_PLAZA.PLAZA_NAME for LOCATION */
    begin
      select PLAZA_NAME into DM_AWAY_AGENCY_TX_TNSP_tab(i).LOCATION from PA_PLAZA 
      where PLAZA_ID=DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAZA_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_AWAY_AGENCY_TX_TNSP_tab(i).LOCATION:=null;
    end;

    /* get PA_PLAZA.AGENCY_ID for PLAZA_AGENCY_ID */
	
    BEGIN
      SELECT io.AGENCY_ID
      INTO DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAZA_AGENCY_ID
      FROM PA_PLAZA pl,
        ST_INTEROP_AGENCIES io
      WHERE DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAZA_ID = pl.plaza_id
      AND pl.AUTHCODE_AUTHORITY_CODE             = io.AUTHORITY_CODE
      AND rownum                                <=1;
    EXCEPTION
    WHEN OTHERS THEN
      NULL;
      DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAZA_AGENCY_ID:=NULL;
    END;


    /* get UFM_LANE_TXN_INFO.HOST_UFM_TOKEN for TX_EXTERN_REF_NO */
    begin
      select HOST_UFM_TOKEN into DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_EXTERN_REF_NO from UFM_LANE_TXN_INFO 
      where TXN_ID=DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_TX_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_EXTERN_REF_NO:=null;
    end;

	/* get COUNTRY_STATE_LOOKUP.COUNTRY for PLATE_COUNTRY */
    begin
      select COUNTRY into DM_AWAY_AGENCY_TX_TNSP_tab(i).PLATE_COUNTRY from COUNTRY_STATE_LOOKUP 
      where STATE_ABBR=DM_AWAY_AGENCY_TX_TNSP_tab(i).PLATE_STATE
            and rownum<=1;
      exception 
        when others then null;
        DM_AWAY_AGENCY_TX_TNSP_tab(i).PLATE_COUNTRY:='USA';
    end;
	
	
    end loop;


    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_AWAY_AGENCY_TX_TNSP_tab.count loop
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_TIMESTAMP is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_TIMESTAMP:=sysdate;
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_MOD_SEQ is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_MOD_SEQ:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_TYPE_IND is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_TYPE_IND:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_SUBTYPE_IND is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_SUBTYPE_IND:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TOLL_SYSTEM_TYPE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TOLL_SYSTEM_TYPE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_MODE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_MODE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_TYPE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_TYPE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_STATE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_STATE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_HEALTH is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_HEALTH:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAZA_AGENCY_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAZA_AGENCY_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAZA_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAZA_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).COLLECTOR_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).COLLECTOR_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TOUR_SEGMENT_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TOUR_SEGMENT_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_DATA_SOURCE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_DATA_SOURCE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_LANE_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_LANE_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_PLAZA_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_PLAZA_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_TIMESTAMP is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_TIMESTAMP:=sysdate;
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_TX_SEQ_NUMBER is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_TX_SEQ_NUMBER:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_VEHICLE_SPEED is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_VEHICLE_SPEED:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_TX_STATUS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_TX_STATUS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_TX_TYPE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_TX_TYPE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TOLL_REVENUE_TYPE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TOLL_REVENUE_TYPE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ACTUAL_CLASS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ACTUAL_CLASS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ACTUAL_AXLES is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ACTUAL_AXLES:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ACTUAL_EXTRA_AXLES is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ACTUAL_EXTRA_AXLES:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).COLLECTOR_CLASS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).COLLECTOR_CLASS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).COLLECTOR_AXLES is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).COLLECTOR_AXLES:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PRECLASS_CLASS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PRECLASS_CLASS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PRECLASS_AXLES is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PRECLASS_AXLES:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).POSTCLASS_CLASS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).POSTCLASS_CLASS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).POSTCLASS_AXLES is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).POSTCLASS_AXLES:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).FORWARD_AXLES is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).FORWARD_AXLES:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).REVERSE_AXLES is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).REVERSE_AXLES:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).FULL_FARE_AMOUNT is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).FULL_FARE_AMOUNT:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DISCOUNTED_AMOUNT is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DISCOUNTED_AMOUNT:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).COLLECTED_AMOUNT is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).COLLECTED_AMOUNT:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).UNREALIZED_AMOUNT is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).UNREALIZED_AMOUNT:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).IS_DISCOUNTABLE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).IS_DISCOUNTABLE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).IS_MEDIAN_FARE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).IS_MEDIAN_FARE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).IS_PEAK is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).IS_PEAK:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PRICE_SCHEDULE_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PRICE_SCHEDULE_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).VEHICLE_SPEED is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).VEHICLE_SPEED:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).RECEIPT_ISSUED is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).RECEIPT_ISSUED:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_NO is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_NO:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ACCOUNT_TYPE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ACCOUNT_TYPE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_CODED_CLASS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_CODED_CLASS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_AGENCY_CLASS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_AGENCY_CLASS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_IAG_CLASS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_IAG_CLASS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_AXLES is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_AXLES:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ACCOUNT_AGENCY_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ACCOUNT_AGENCY_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).READ_AVI_CLASS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).READ_AVI_CLASS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).READ_AVI_AXLES is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).READ_AVI_AXLES:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_PROGRAM_STATUS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_PROGRAM_STATUS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).BUFFERED_READ_FLAG is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).BUFFERED_READ_FLAG:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_DEVICE_STATUS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_DEVICE_STATUS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).POST_DEVICE_STATUS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).POST_DEVICE_STATUS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PRE_TXN_BALANCE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PRE_TXN_BALANCE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAN_TYPE_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PLAN_TYPE_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ETC_TX_STATUS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ETC_TX_STATUS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).SPEED_VIOL_FLAG is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).SPEED_VIOL_FLAG:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).IMAGE_TAKEN is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).IMAGE_TAKEN:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PLATE_COUNTRY is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PLATE_COUNTRY:='USA';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PLATE_STATE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PLATE_STATE:='FL';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).PLATE_NUMBER is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).PLATE_NUMBER:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).REVENUE_DATE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).REVENUE_DATE:=sysdate;
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).POSTED_DATE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).POSTED_DATE:=sysdate;
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ATP_FILE_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ATP_FILE_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).UPDATE_TS is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).UPDATE_TS:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).IS_REVERSED is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).IS_REVERSED:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).CORR_REASON_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).CORR_REASON_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).RECON_DATE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).RECON_DATE:=sysdate;
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).RECON_STATUS_IND is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).RECON_STATUS_IND:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).RECON_SUB_CODE_IND is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).RECON_SUB_CODE_IND:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).EXTERN_FILE_DATE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).EXTERN_FILE_DATE:=sysdate;
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).MILEAGE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).MILEAGE:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_READ_COUNT is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_READ_COUNT:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_WRITE_COUNT is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DEVICE_WRITE_COUNT:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_DEVICE_READ_COUNT is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_DEVICE_READ_COUNT:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_DEVICE_WRITE_COUNT is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).ENTRY_DEVICE_WRITE_COUNT:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).DEPOSIT_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).DEPOSIT_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_DATE is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_DATE:=sysdate;
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LOCATION is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LOCATION:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_SEQ_NO is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_SEQ_NO:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_EXTERN_REF_NO is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_EXTERN_REF_NO:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_SEQ_NUMBER is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).TX_SEQ_NUMBER:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).EXTERN_FILE_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).EXTERN_FILE_ID:='0';
         end if;
	 if DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_ID is null then
          DM_AWAY_AGENCY_TX_TNSP_tab(i).LANE_ID:='0';
         end if;
    end loop;



    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_AWAY_AGENCY_TX_TNSP_tab.first .. DM_AWAY_AGENCY_TX_TNSP_tab.last
           INSERT INTO DM_AWAY_AGENCY_TX_TNSP_INFO VALUES DM_AWAY_AGENCY_TX_TNSP_tab(i);
                       
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


