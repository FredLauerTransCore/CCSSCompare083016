/********************************************************
*
* Name: DM_TOLL_EXCEPTIONS_TRAN_PROC
* Created by: DT, 5/2/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_TOLL_EXCEPTIONS_TRANSP_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_TOLL_EXCEPTIONS_TRAN_PROC IS

TYPE DM_TOLL_EXCEPTIONS_TRAN_TYP IS TABLE OF DM_TOLL_EXCEPTIONS_TRANSP_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_TOLL_EXCEPTIONS_TRAN_tab DM_TOLL_EXCEPTIONS_TRAN_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    TXN_ID TX_EXTERN_REF_NO
    ,EXT_MSG_SEQ_NUM TX_SEQ_NUMBER
    ,'0' EXTERN_FILE_ID
    ,EXT_LANE_ID LANE_ID
    ,EXT_DATE_TIME TX_TIMESTAMP
    ,decode(trans_source,null,0,1) TX_MOD_SEQ
    ,'R' TX_TYPE_IND
    ,'T' TX_SUBTYPE_IND
    ,decode(substr(ext_plaza_id,1,3),'004','C','B') TOLL_SYSTEM_TYPE
    ,EXT_LANE_TYPE_CODE LANE_MODE
    ,'1' LANE_TYPE
    ,'0' LANE_STATE
    ,'0' LANE_HEALTH
    ,(select io.AGENCY_ID
        from PA_LANE_TXN ln, PA_PLAZA pl, ST_INTEROP_AGENCIES io
        where ln.ext_plaza_id  = pl.plaza_id
        and pl.AUTHCODE_AUTHORITY_CODE = io.AUTHORITY_CODE)
        PLAZA_AGENCY_ID
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
    ,'0' DISCOUNTED_AMOUNT
    ,'0' COLLECTED_AMOUNT
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
    ,'0' ACCOUNT_AGENCY_ID
    ,TRANSP_CLASS READ_AVI_CLASS
    ,'0' READ_AVI_AXLES
    ,'N' DEVICE_PROGRAM_STATUS
    ,'N' BUFFERED_READ_FLAG
    ,MSG_INVALID LANE_DEVICE_STATUS
    ,'0' POST_DEVICE_STATUS
    ,'0' PRE_TXN_BALANCE
    ,'0' PLAN_TYPE_ID
    ,'0' ETC_TX_STATUS
    ,'F' SPEED_VIOL_FLAG
    ,'N' IMAGE_TAKEN
    ,'**' PLATE_COUNTRY
    ,'**' PLATE_STATE
    ,'**' PLATE_NUMBER
    ,trunc(EXT_DATE_TIME) REVENUE_DATE
    ,trunc(TXN_PROCESS_DATE) POSTED_DATE
    ,'0' ATP_FILE_ID
    ,TXN_PROCESS_DATE UPDATE_TS
    ,'N' IS_REVERSED
    ,'0' CORR_REASON_ID
    ,trunc(TXN_PROCESS_DATE) RECON_DATE
    ,'0' RECON_STATUS_IND
    ,'1' RECON_SUB_CODE_IND
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
    ,'SUNPASS' SOURCE_SYSTEM
FROM SUNPASS.PA_LANE_TXN_REJECT;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_TOLL_EXCEPTIONS_TRAN_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    FOR i in 1 .. DM_TOLL_EXCEPTIONS_TRAN_tab.count loop

    /* get PA_PLAZA.Join ST_INTEROP_AGENCIES and PA_PLAZA on ENT_PLAZA_ID to PLAZA_ID for PLAZA_AGENCY_ID */
    begin
      select Join ST_INTEROP_AGENCIES and PA_PLAZA on ENT_PLAZA_ID to PLAZA_ID into DM_TOLL_EXCEPTIONS_TRAN_tab(i).PLAZA_AGENCY_ID from PA_PLAZA 
      where TXN_ID=DM_TOLL_EXCEPTIONS_TRAN_tab(i).TX_EXTERN_REF_NO
            and rownum<=1;
      exception 
        when others then null;
        DM_TOLL_EXCEPTIONS_TRAN_tab(i).PLAZA_AGENCY_ID:=null;
    end;

    /* get PA_LANE_TXN_REJECT.JOIN PA_PLAZA ON PLAZA_ID RETURN PLAZA_NAME for LOCATION */
    begin
      select JOIN PA_PLAZA ON PLAZA_ID RETURN PLAZA_NAME into DM_TOLL_EXCEPTIONS_TRAN_tab(i).LOCATION from PA_LANE_TXN_REJECT 
      where PLAZA_ID=DM_TOLL_EXCEPTIONS_TRAN_tab(i).PLAZA_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TOLL_EXCEPTIONS_TRAN_tab(i).LOCATION:=null;
    end;

    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_TOLL_EXCEPTIONS_TRAN_tab.first .. DM_TOLL_EXCEPTIONS_TRAN_tab.last
           INSERT INTO DM_TOLL_EXCEPTIONS_TRANSP_INFO VALUES DM_TOLL_EXCEPTIONS_TRAN_tab(i);
                       
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


