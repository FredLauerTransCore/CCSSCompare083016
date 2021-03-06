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
	--,EXT_LANE_TYPE_CODE LANE_MODE
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
    ,NULL ACCOUNT_TYPE
    ,TRANSP_CLASS DEVICE_CODED_CLASS
    ,'0' DEVICE_AGENCY_CLASS
    ,'0' DEVICE_IAG_CLASS
    ,'0' DEVICE_AXLES
    ,NULL ETC_ACCOUNT_ID
	--,'FTE' ACCOUNT_AGENCY_ID
    ,'101' ACCOUNT_AGENCY_ID
    ,TRANSP_CLASS READ_AVI_CLASS
    ,'0' READ_AVI_AXLES
    ,'N' DEVICE_PROGRAM_STATUS
    ,'N' BUFFERED_READ_FLAG
    ,MSG_INVALID LANE_DEVICE_STATUS
    ,'0' POST_DEVICE_STATUS
    ,'0' PRE_TXN_BALANCE
    ,'1' PLAN_TYPE_ID
    ,'9999' ETC_TX_STATUS
    ,'F' SPEED_VIOL_FLAG
    ,decode(msg_id,'TTOL','Y','N') IMAGE_TAKEN
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
WHERE (TRANSP_ID like  '%0110' or 
   TRANSP_ID like '%0210') AND ( EXT_PLAZA_ID NOT LIKE '200%' AND EXT_PLAZA_ID NOT LIKE '0008%'  AND EXT_PLAZA_ID not LIKE '30001%');
   
   v_FTE_PLAZA varchar2(1);
   
BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_TOLL_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */



    FOR i in 1 .. DM_ACCOUNT_TOLL_INFO_tab.count loop


    begin
      select HOST_UFM_TOKEN into DM_ACCOUNT_TOLL_INFO_tab(i).TX_EXTERN_REF_NO from UFM_LANE_TXN_INFO 
      where TXN_ID=DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TX_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).TX_EXTERN_REF_NO:=null;
    end;


    /* get PA_PLAZA.AGENCY_ID for PLAZA_AGENCY_ID */
    BEGIN
      SELECT io.AGENCY_ID
      INTO DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_AGENCY_ID
      FROM PA_PLAZA pl,
        ST_INTEROP_AGENCIES io
      WHERE DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID = pl.plaza_id
      AND pl.AUTHCODE_AUTHORITY_CODE             = io.AUTHORITY_CODE
      AND rownum                                <=1;
    EXCEPTION
	when no_data_found then
	 begin
		 SELECT io.AGENCY_ID
         INTO DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_AGENCY_ID
         FROM PA_PLAZA pl,
          ST_INTEROP_AGENCIES io
         WHERE DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID = pl.plaza_id
	     AND io.AGENCY_NAME='FTE' and pl.FTE_PLAZA='Y'
         AND rownum                                <=1;
	  exception when others then null;
	     DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_AGENCY_ID:=NULL;
	  end;
    WHEN OTHERS THEN
      DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_AGENCY_ID:=NULL;
    END;

    /* get PA_STATE_CODE.STATE_CODE_ABB for PLATE_STATE */
    begin
      select STATE_CODE_ABBR into DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_STATE from PA_STATE_CODE sc, pa_lane_txn ln
      where sc.STATE_CODE_NUM=ln.state_id_code and DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TX_ID=ln.txn_id
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


    /* get PA_PLAZA.FACCODE_FACILITY_CODE for FACILITY_ID */
    begin
      select pp.FACCODE_FACILITY_CODE
             ,pp.PLAZA_NAME
             ,case WHEN pp.FTE_PLAZA = 'N' THEN 'I'
                   WHEN lt.MSG_ID = 'ITOL' THEN 'V'
                   ELSE 'E'
              end
	    into 
	    DM_ACCOUNT_TOLL_INFO_tab(i).FACILITY_ID ,
	    DM_ACCOUNT_TOLL_INFO_tab(i).LOCATION,
	    DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND
	    from PA_PLAZA pp,
           PA_LANE_TXN lt
      where pp.PLAZA_ID=DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID
            and lt.EXT_PLAZA_ID=DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).FACILITY_ID:=null;
    end;


    begin
      BEGIN
        SELECT fte_plaza
        INTO v_fte_plaza
        FROM pa_plaza
        WHERE plaza_id=DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID;
        IF v_fte_plaza='N' THEN
          DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND:='I';
        ELSE
          IF DM_ACCOUNT_TOLL_INFO_tab(i).IMAGE_TAKEN='Y' THEN
            DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND:='V';
          ELSE
            DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND:='V';
          END IF;
        END IF;
      EXCEPTION
      WHEN OTHERS THEN
         NULL;
      END;
      select case WHEN DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND = 'E' THEN 'Z'
                  WHEN DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND = 'V' THEN 
                       case WHEN DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_NO like '%0210' THEN 'I'
                            ELSE 'T'
                       end
                  WHEN DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND = 'I' THEN
                       case WHEN lt.MSG_ID = 'TTOL' THEN 'C'
                            ELSE 'I'
                       end
             end
	    into 
	    DM_ACCOUNT_TOLL_INFO_tab(i).TX_SUBTYPE_IND
	    from PA_LANE_TXN lt
      where lt.EXT_PLAZA_ID=DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID
        and rownum<=1;
      exception 
        when others then null;
    end;


    /* derive ETC_TX_STATUS from TX_TYPE_IND and TX_SUBTYPE_IND */
    begin
      select decode(DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND||DM_ACCOUNT_TOLL_INFO_tab(i).TX_SUBTYPE_IND, 
                                                 'EZ', 1,
                                                 'VT', 2,
                                                 'VI', 9,
                                                 'IC', 7,
                                                 'II', 6,
                                                       '9999' )
	    into 
	    DM_ACCOUNT_TOLL_INFO_tab(i).ETC_TX_STATUS
	    from PA_PLAZA 
      where PLAZA_ID=DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID
        and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_TOLL_INFO_tab(i).ETC_TX_STATUS:='9999';
    end;


       /* to default the values NOT NULL columns */

	 if DM_ACCOUNT_TOLL_INFO_tab(i).FULL_FARE_AMOUNT is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).FULL_FARE_AMOUNT:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).DISCOUNTED_AMOUNT is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).DISCOUNTED_AMOUNT:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).COLLECTED_AMOUNT is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).COLLECTED_AMOUNT:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).UNREALIZED_AMOUNT is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).UNREALIZED_AMOUNT:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).IS_MEDIAN_FARE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).IS_MEDIAN_FARE:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).IS_PEAK is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).IS_PEAK:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).VEHICLE_SPEED is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).VEHICLE_SPEED:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_NO is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_NO:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_CODED_CLASS is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_CODED_CLASS:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_AGENCY_CLASS is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_AGENCY_CLASS:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_AXLES is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_AXLES:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ACCOUNT_AGENCY_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ACCOUNT_AGENCY_ID:='101';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).READ_AVI_CLASS is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).READ_AVI_CLASS:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).READ_AVI_AXLES is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).READ_AVI_AXLES:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_PROGRAM_STATUS is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).DEVICE_PROGRAM_STATUS:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).BUFFERED_READ_FLAG is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).BUFFERED_READ_FLAG:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).LANE_DEVICE_STATUS is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).LANE_DEVICE_STATUS:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).PRE_TXN_BALANCE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).PRE_TXN_BALANCE:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).PLAN_TYPE_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).PLAN_TYPE_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ETC_TX_STATUS is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ETC_TX_STATUS:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).SPEED_VIOL_FLAG is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).SPEED_VIOL_FLAG:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).IMAGE_TAKEN is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).IMAGE_TAKEN:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_COUNTRY is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_COUNTRY:='USA';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_STATE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_STATE:='FL';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_NUMBER is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).PLATE_NUMBER:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).REVENUE_DATE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).REVENUE_DATE:=sysdate;
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).POSTED_DATE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).POSTED_DATE:=sysdate;
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).UPDATE_TS is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).UPDATE_TS:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).IS_REVERSED is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).IS_REVERSED:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).CORR_REASON_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).CORR_REASON_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).RECON_DATE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).RECON_DATE:=sysdate;
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).EXTERN_FILE_DATE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).EXTERN_FILE_DATE:=sysdate;
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).TX_DATE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).TX_DATE:=sysdate;
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).LOCATION is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).LOCATION:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).FACILITY_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).FACILITY_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).TX_EXTERN_REF_NO is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).TX_EXTERN_REF_NO:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).TX_SEQ_NUMBER is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).TX_SEQ_NUMBER:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).EXTERN_FILE_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).EXTERN_FILE_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).LANE_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).LANE_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).TX_TIMESTAMP is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).TX_TIMESTAMP:=sysdate;
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).TX_TYPE_IND:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).TX_SUBTYPE_IND is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).TX_SUBTYPE_IND:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).TOLL_SYSTEM_TYPE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).TOLL_SYSTEM_TYPE:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).LANE_MODE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).LANE_MODE:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TYPE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TYPE:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_AGENCY_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_AGENCY_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).PLAZA_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).COLLECTOR_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).COLLECTOR_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).TOUR_SEGMENT_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).TOUR_SEGMENT_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_LANE_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_LANE_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_PLAZA_ID is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_PLAZA_ID:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_TIMESTAMP is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_TIMESTAMP:=sysdate;
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_TX_SEQ_NUMBER is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_TX_SEQ_NUMBER:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_VEHICLE_SPEED is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ENTRY_VEHICLE_SPEED:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TX_STATUS is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TX_STATUS:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TX_TYPE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).LANE_TX_TYPE:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).TOLL_REVENUE_TYPE is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).TOLL_REVENUE_TYPE:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ACTUAL_CLASS is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ACTUAL_CLASS:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ACTUAL_AXLES is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ACTUAL_AXLES:='0';
         end if;
	 if DM_ACCOUNT_TOLL_INFO_tab(i).ACTUAL_EXTRA_AXLES is null then
          DM_ACCOUNT_TOLL_INFO_tab(i).ACTUAL_EXTRA_AXLES:='0';
         end if;
    end loop;

   
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



