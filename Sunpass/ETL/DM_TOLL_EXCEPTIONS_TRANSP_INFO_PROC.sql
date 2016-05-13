/********************************************************
*
* Name: DM_TOLL_EXCEPTIONS_TRANSP_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_TOLL_EXCEPTIONS_TRANSP_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_TOLL_EXCEPTIONS_TRANSP_INFO_PROC IS

TYPE DM_TOLL_EXCEPTIONS_TRANSP_INFO_TYP IS TABLE OF DM_TOLL_EXCEPTIONS_TRANSP_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_TOLL_EXCEPTIONS_TRANSP_INFO_tab DM_TOLL_EXCEPTIONS_TRANSP_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL TX_EXTERN_REF_NO
    ,NULL TX_SEQ_NUMBER
    ,NULL EXTERN_FILE_ID
    ,NULL LANE_ID
    ,NULL TX_TIMESTAMP
    ,NULL TX_MOD_SEQ
    ,NULL TX_TYPE_IND
    ,NULL TX_SUBTYPE_IND
    ,NULL TOLL_SYSTEM_TYPE
    ,NULL LANE_MODE
    ,NULL LANE_TYPE
    ,NULL LANE_STATE
    ,NULL LANE_HEALTH
    ,NULL PLAZA_AGENCY_ID
    ,NULL PLAZA_ID
    ,NULL COLLECTOR_ID
    ,NULL TOUR_SEGMENT_ID
    ,NULL ENTRY_DATA_SOURCE
    ,NULL ENTRY_LANE_ID
    ,NULL ENTRY_PLAZA_ID
    ,NULL ENTRY_TIMESTAMP
    ,NULL ENTRY_TX_SEQ_NUMBER
    ,NULL ENTRY_VEHICLE_SPEED
    ,NULL LANE_TX_STATUS
    ,NULL LANE_TX_TYPE
    ,NULL TOLL_REVENUE_TYPE
    ,NULL ACTUAL_CLASS
    ,NULL ACTUAL_AXLES
    ,NULL ACTUAL_EXTRA_AXLES
    ,NULL COLLECTOR_CLASS
    ,NULL COLLECTOR_AXLES
    ,NULL PRECLASS_CLASS
    ,NULL PRECLASS_AXLES
    ,NULL POSTCLASS_CLASS
    ,NULL POSTCLASS_AXLES
    ,NULL FORWARD_AXLES
    ,NULL REVERSE_AXLES
    ,NULL FULL_FARE_AMOUNT
    ,NULL DISCOUNTED_AMOUNT
    ,NULL COLLECTED_AMOUNT
    ,NULL UNREALIZED_AMOUNT
    ,NULL IS_DISCOUNTABLE
    ,NULL IS_MEDIAN_FARE
    ,NULL IS_PEAK
    ,NULL PRICE_SCHEDULE_ID
    ,NULL VEHICLE_SPEED
    ,NULL RECEIPT_ISSUED
    ,NULL DEVICE_NO
    ,NULL ACCOUNT_TYPE
    ,NULL DEVICE_CODED_CLASS
    ,NULL DEVICE_AGENCY_CLASS
    ,NULL DEVICE_IAG_CLASS
    ,NULL DEVICE_AXLES
    ,NULL ETC_ACCOUNT_ID
    ,NULL ACCOUNT_AGENCY_ID
    ,NULL READ_AVI_CLASS
    ,NULL READ_AVI_AXLES
    ,NULL DEVICE_PROGRAM_STATUS
    ,NULL BUFFERED_READ_FLAG
    ,NULL LANE_DEVICE_STATUS
    ,NULL POST_DEVICE_STATUS
    ,NULL PRE_TXN_BALANCE
    ,NULL PLAN_TYPE_ID
    ,NULL ETC_TX_STATUS
    ,NULL SPEED_VIOL_FLAG
    ,NULL IMAGE_TAKEN
    ,NULL PLATE_COUNTRY
    ,NULL PLATE_STATE
    ,NULL PLATE_NUMBER
    ,NULL REVENUE_DATE
    ,NULL POSTED_DATE
    ,NULL ATP_FILE_ID
    ,NULL UPDATE_TS
    ,NULL IS_REVERSED
    ,NULL CORR_REASON_ID
    ,NULL RECON_DATE
    ,NULL RECON_STATUS_IND
    ,NULL RECON_SUB_CODE_IND
    ,NULL EXTERN_FILE_DATE
    ,NULL MILEAGE
    ,NULL DEVICE_READ_COUNT
    ,NULL DEVICE_WRITE_COUNT
    ,NULL ENTRY_DEVICE_READ_COUNT
    ,NULL ENTRY_DEVICE_WRITE_COUNT
    ,NULL DEPOSIT_ID
    ,NULL TX_DATE
    ,NULL LOCATION
    ,NULL FARE_TBL_ID
    ,NULL SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_TOLL_EXCEPTIONS_TRANSP_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_TOLL_EXCEPTIONS_TRANSP_INFO_tab.first .. DM_TOLL_EXCEPTIONS_TRANSP_INFO_tab.last
           INSERT INTO DM_TOLL_EXCEPTIONS_TRANSP_INFO VALUES DM_TOLL_EXCEPTIONS_TRANSP_INFO_tab(i);
                       
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


