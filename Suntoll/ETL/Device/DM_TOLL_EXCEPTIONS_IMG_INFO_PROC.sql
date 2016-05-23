/********************************************************
*
* Name: DM_TOLL_EXCEPTIONS_IMG_INFO_PROC
* Created by: RH, 5/22/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_TOLL_EXCEPTIONS_IMG_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_TOLL_EXCEPTIONS_IMG_INFO_PROC IS

TYPE DM_TOLL_EXCEPTIONS_IMG_INFO_TYP IS TABLE OF DM_TOLL_EXCEPTIONS_IMG_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_TOLL_EXCEPTIONS_IMG_INFO_tab DM_TOLL_EXCEPTIONS_IMG_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    TXN_ID TX_EXTERN_REF_NO
    ,EXT_MSG_SEQ_NUM TX_SEQ_NUMBER
    ,0 EXTERN_FILE_ID
    ,EXT_LANE_ID LANE_ID
    ,EXT_DATE_TIME TX_TIMESTAMP
    ,NULL TX_MOD_SEQ  -- Derived
    ,'F' TX_TYPE_IND
    ,'Z' TX_SUBTYPE_IND
    ,NULL TOLL_SYSTEM_TYPE  -- Derived
    ,EXT_LANE_TYPE_CODE LANE_MODE
    ,1 LANE_TYPE
    ,0 LANE_STATE
    ,0 LANE_HEALTH
    ,NULL PLAZA_AGENCY_ID --PA_PLAZA.AGENCY_ID
    ,NULL PLAZA_ID  -- PA_PLAZA.EXT_PLAZA_ID
    ,0 COLLECTOR_ID
    ,0 TOUR_SEGMENT_ID
    ,0 ENTRY_DATA_SOURCE
    ,0 ENTRY_LANE_ID
    ,0 ENTRY_PLAZA_ID
    ,0 ENTRY_TIMESTAMP
    ,0 ENTRY_TX_SEQ_NUMBER
    ,0 ENTRY_VEHICLE_SPEED
    ,0 LANE_TX_STATUS
    ,0 LANE_TX_TYPE
    ,0 TOLL_REVENUE_TYPE
    ,0 ACTUAL_CLASS
    ,0 ACTUAL_AXLES
    ,0 ACTUAL_EXTRA_AXLES
    ,0 COLLECTOR_CLASS
    ,0 COLLECTOR_AXLES
    ,0 PRECLASS_CLASS
    ,0 PRECLASS_AXLES
    ,0 POSTCLASS_CLASS
    ,0 POSTCLASS_AXLES
    ,0 FORWARD_AXLES
    ,0 REVERSE_AXLES
    ,0 FULL_FARE_AMOUNT
    ,0 DISCOUNTED_AMOUNT
    ,0 COLLECTED_AMOUNT
    ,0 UNREALIZED_AMOUNT
    ,0 IS_DISCOUNTABLE
    ,0 IS_MEDIAN_FARE
    ,0 IS_PEAK
    ,0 PRICE_SCHEDULE_ID
    ,0 VEHICLE_SPEED
    ,0 RECEIPT_ISSUED
    ,0 DEVICE_NO
    ,0 ACCOUNT_TYPE
    ,0 DEVICE_CODED_CLASS
    ,0 DEVICE_AGENCY_CLASS
    ,0 DEVICE_IAG_CLASS
    ,0 DEVICE_AXLES
    ,0 ETC_ACCOUNT_ID
    ,0 ACCOUNT_AGENCY_ID
    ,0 READ_AVI_CLASS
    ,0 READ_AVI_AXLES
    ,0 DEVICE_PROGRAM_STATUS
    ,0 BUFFERED_READ_FLAG
    ,0 LANE_DEVICE_STATUS
    ,0 POST_DEVICE_STATUS
    ,0 PRE_TXN_BALANCE
    ,0 PLAN_TYPE_ID
    ,0 ETC_TX_STATUS
    ,0 SPEED_VIOL_FLAG
    ,0 IMAGE_TAKEN
    ,0 PLATE_COUNTRY
    ,0 PLATE_STATE
    ,0 PLATE_NUMBER
    ,0 REVENUE_DATE  -- date?
    ,0 POSTED_DATE  -- date?
    ,0 ATP_FILE_ID
    ,0 UPDATE_TS
    ,0 IS_REVERSED
    ,0 CORR_REASON_ID
    ,0 RECON_DATE  -- date?
    ,0 RECON_STATUS_IND
    ,0 RECON_SUB_CODE_IND
    ,0 EXTERN_FILE_DATE  -- date?
    ,0 MILEAGE
    ,0 DEVICE_READ_COUNT
    ,0 DEVICE_WRITE_COUNT
    ,0 ENTRY_DEVICE_READ_COUNT
    ,0 ENTRY_DEVICE_WRITE_COUNT
    ,0 DEPOSIT_ID
    ,0 TX_DATE  -- date?
    ,0 LOCATION
    ,0 FARE_TBL_ID
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PA_LANE_TXN_REJECT; -- Source

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_TOLL_EXCEPTIONS_IMG_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_TOLL_EXCEPTIONS_IMG_INFO_tab.first .. DM_TOLL_EXCEPTIONS_IMG_INFO_tab.last
           INSERT INTO DM_TOLL_EXCEPTIONS_IMG_INFO VALUES DM_TOLL_EXCEPTIONS_IMG_INFO_tab(i);
                       
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


