/********************************************************
*
* Name: DM_TRIP_HISTORY_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_TRIP_HISTORY_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_TRIP_HISTORY_INFO_PROC IS

TYPE DM_TRIP_HISTORY_INFO_TYP IS TABLE OF DM_TRIP_HISTORY_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_TRIP_HISTORY_INFO_tab DM_TRIP_HISTORY_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
rownum	APD_ID
,EXIT_MONTH	TRIP_START_DATE
,EXIT_MONTH	TRIP_END_DATE
,last_day(exit_month)+13	RECON_DATE
,USAGE_COUNT	TRIPS_MADE
,USAGE_COUNT	TRIPS_CHARGED
,'0'	LATE_TRIPS
,REBATE_AMOUNT	AMOUNT_CHARGED
,'Y'	RECON_FLAG
,'0'	TRIPS_LEFT
,last_day(exit_month)+13	UPDATE_TS
,last_day(sysdate)	LAST_TX_DATE
,TOTAL_TOLL_AMT	USAGE_AMOUNT
,TOTAL_TOLL_AMT	FULL_FARE_AMOUNT
,TOTAL_TOLL_AMT	DISCOUNTED_AMOUNT
,ACCT_NUM	ETC_ACCOUNT_ID
,REBATE_REBATE_PROGRAM_CODE	PLAN_TYPE
,'0'	LAST_LANE_TX_ID
,last_day(sysdate)	LAST_TX_POST_TIMESTAMP
,NULL	COMPOSITE_TRANSACTION_ID
,'SUNPASS'	SOURCE_SYSTEM
,TRANSP_ID	TRANSPONDER_ID
FROM PA_REBATE_AUTHORITY;



BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_TRIP_HISTORY_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN*/
	
	FOR i in 1 .. DM_TRIP_HISTORY_INFO_tab.count loop
	
	begin
	select pur_id into DM_TRIP_HISTORY_INFO_tab(i).COMPOSITE_TRANSACTION_ID 
	from pa_purchase pp, pa_purchase_detail ppd where 
    PUR_PUR_ID=PUR_ID and INVTRANSP_TRANSP_TRANSP_ID=DM_TRIP_HISTORY_INFO_tab(i).TRANSPONDER_ID;
	
	exception 
        when others then null;
        DM_TRIP_HISTORY_INFO_tab(i).COMPOSITE_TRANSACTION_ID:=0;
    end;
	end loop;

    /*ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_TRIP_HISTORY_INFO_tab.first .. DM_TRIP_HISTORY_INFO_tab.last
           INSERT INTO DM_TRIP_HISTORY_INFO VALUES DM_TRIP_HISTORY_INFO_tab(i);
                       
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

