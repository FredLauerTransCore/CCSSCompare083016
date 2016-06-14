/********************************************************
*
* Name: DM_ACCOUNT_EPS_TOLL_INF_PROC
* Created by: DT, 5/3/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCOUNT_EPS_TOLL_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_ACCOUNT_EPS_TOLL_INF_PROC IS

TYPE DM_ACCOUNT_EPS_TOLL_INF_TYP IS TABLE OF DM_ACCOUNT_EPS_TOLL_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_EPS_TOLL_INF_tab DM_ACCOUNT_EPS_TOLL_INF_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL TX_EXTERN_REF_NO
    ,EXT_MSG_SEQ_NUM TX_SEQ_NUMBER
    ,'0' EXTERN_FILE_ID
    ,EXT_LANE_ID LANE_ID
    ,EXT_DATE_TIME TX_TIMESTAMP
    ,decode(trans_source,null,0,1) TX_MOD_SEQ
    ,NULL TX_TYPE_IND
    ,'Z' TX_SUBTYPE_IND
    ,'C' TOLL_SYSTEM_TYPE
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
    ,'101' ACCOUNT_AGENCY_ID
    ,TRANSP_CLASS READ_AVI_CLASS
    ,'0' READ_AVI_AXLES
    ,'N' DEVICE_PROGRAM_STATUS
    ,'N' BUFFERED_READ_FLAG
    ,MSG_INVALID LANE_DEVICE_STATUS
    ,'0' POST_DEVICE_STATUS
    ,NULL PRE_TXN_BALANCE
    ,'1' PLAN_TYPE_ID
    ,'5' ETC_TX_STATUS
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
    ,to_date('01/01/1900','MM/DD/YYYY') EXTERN_FILE_DATE
    ,'0' MILEAGE
    ,'0' DEVICE_READ_COUNT
    ,'0' DEVICE_WRITE_COUNT
    ,'0' ENTRY_DEVICE_READ_COUNT
    ,'0' ENTRY_DEVICE_WRITE_COUNT
    ,'000000000000' DEPOSIT_ID
    ,NULL BANK_AUTH_NUMBER
    ,NULL CRCARD_EXPIRY
    ,'1' EPS_REVENUE_TYPE
    ,NULL REBILL_ACCOUNT_NUMBER
    ,NULL REBILL_ACCOUNT_NUMBER_ENCRYPT
    ,NULL REBILL_PAY_TYPE
    ,TXN_PROCESS_DATE PAYMENT_POSTED_DATE
    ,trunc(EXT_DATE_TIME) TX_DATE
    ,'0' FARE_TBL_ID
    ,'SUNPASS' SOURCE_SYSTEM
    ,TXN_ID LANE_TX_ID
FROM PA_LANE_TXN WHERE TRANSP_ID like '%0110'  AND (ENT_PLAZA_ID LIKE '200%' OR ENT_PLAZA_ID LIKE '008%');

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_EPS_TOLL_INF_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in 1 .. DM_ACCOUNT_EPS_TOLL_INF_tab.count loop

    /* get PARKING_REF.REF_TXN_ID for TX_EXTERN_REF_NO */
    begin
      select REF_TXN_ID into DM_ACCOUNT_EPS_TOLL_INF_tab(i).TX_EXTERN_REF_NO from PARKING_REF 
      where TXN_ID=DM_ACCOUNT_EPS_TOLL_INF_tab(i).TXN_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_EPS_TOLL_INF_tab(i).TX_EXTERN_REF_NO:=null;
    end;

    /* get PA_PLAZA.AGENCY_ID for PLAZA_AGENCY_ID */
    begin
      select AGENCY_ID into DM_ACCOUNT_EPS_TOLL_INF_tab(i).PLAZA_AGENCY_ID from PA_PLAZA 
      where PLAZA_ID=DM_ACCOUNT_EPS_TOLL_INF_tab(i).ENT_PLAZA_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_EPS_TOLL_INF_tab(i).PLAZA_AGENCY_ID:=null;
    end;

    /* get st_ledger.ACCT_NUM for ETC_ACCOUNT_ID */
    begin
      select ACCT_NUM into DM_ACCOUNT_EPS_TOLL_INF_tab(i).ETC_ACCOUNT_ID from st_ledger 
      where PA_LANE_TXN_ID=DM_ACCOUNT_EPS_TOLL_INF_tab(i).TXN_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_EPS_TOLL_INF_tab(i).ETC_ACCOUNT_ID:=null;
    end;

    /* get PA_ACCT.ACCTTYPE_ACCT_TYPE_CODE for ACCOUNT_TYPE */
    begin
      select ACCTTYPE_ACCT_TYPE_CODE into DM_ACCOUNT_EPS_TOLL_INF_tab(i).ACCOUNT_TYPE from PA_ACCT 
      where ACCT_NUM=DM_ACCOUNT_EPS_TOLL_INF_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_EPS_TOLL_INF_tab(i).ACCOUNT_TYPE:=null;
    end;

    /* get PA_STATE_CODE.STATE_CODE_ABB for PLATE_STATE */
    begin
      select STATE_CODE_ABB into DM_ACCOUNT_EPS_TOLL_INF_tab(i).PLATE_STATE from PA_STATE_CODE 
      where STATE_CODE_NUM=DM_ACCOUNT_EPS_TOLL_INF_tab(i).STATE_ID_CODE
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_EPS_TOLL_INF_tab(i).PLATE_STATE:=null;
    end;

    end loop;



    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_EPS_TOLL_INF_tab.first .. DM_ACCOUNT_EPS_TOLL_INF_tab.last
           INSERT INTO DM_ACCOUNT_EPS_TOLL_INFO VALUES DM_ACCOUNT_EPS_TOLL_INF_tab(i);
                       
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


