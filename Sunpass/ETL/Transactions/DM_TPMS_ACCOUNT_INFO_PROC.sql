/********************************************************
*
* Name: DM_TPMS_ACCOUNT_INFO_PROC
* Created by: DT, 5/2/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_TPMS_ACCOUNT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_TPMS_ACCOUNT_INFO_PROC IS

TYPE DM_TPMS_ACCOUNT_INFO_TYP IS TABLE OF DM_TPMS_ACCOUNT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_TPMS_ACCOUNT_INFO_tab DM_TPMS_ACCOUNT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    al.ACCT_NUM ETC_ACCOUNT_ID
    ,al.LEDGER_ID LEDGER_ID
    ,'CCSS' AGENCY_ID  --Is a number in target
    ,pa.ACCTTYPE_ACCT_TYPE_CODE ACCOUNT_TYPE
    ,al.ACCT_BALANCE CURRENT_BALANCE
    ,'0' POSTPAID_BALANCE
    ,'0' DEVICE_DEPOSIT
    ,'0' VIOL_PREPAYMENT
    ,'0' VIOL_OVERPAYMENT
    ,'N' IS_DISCOUNT_ELIGIBLE
    ,NULL LAST_TOLL_TX_ID
    ,NULL LAST_TOLL_POSTED_DATE
    ,NULL LAST_TOLL_TX_AMT
    ,NULL LAST_TOLL_TX_TIMESTAMP
    ,NULL FIRST_TOLL_TIMESTAMP
    ,to_date('01/01/1900','MM/DD/YYYY') EVAL_START_DATE
    ,to_date('01/01/1900','MM/DD/YYYY') EVAL_END_DATE
    ,'0' USAGE_AMOUNT
    ,al.LAST_UPDATED UPDATE_TS
    ,pa.ACCT_OPEN_DATE OPEN_DATE
    ,'0' POSTPAID_UNBILLED_BALANCE
    ,'0' VIOL_TOLL_BALANCE
    ,'0' VIOL_FEE_BALANCE
    ,'0' VIOL_BALANCE
    ,NULL POSTPAID_FEE
    ,NULL COLL_TOLL
    ,NULL COLL_FEE
    ,NULL UTC_TOLL
    ,NULL UTC_FEE
    ,NULL COURT_TOLL
    ,NULL COURT_FEE
    ,NULL LAST_TOLL_TX_DATE
    ,NULL LAST_FIN_TX_DATE
    ,'0' ACCT_FIN_STATUS
    ,NULL LAST_PAYMENT_TX_ID
    ,NULL LAST_PAYMENT_DATE
    ,NULL LAST_FIN_TX_ID
    ,NULL LAST_FIN_TX_AMT
    ,NULL LAST_BAL_POS_DATE
    ,NULL LAST_BAL_NEG_DATE
    ,to_date('01/01/1900','MM/DD/YYYY') LAST_VIOL_PAYMENT_DATE
    ,'0' POSTPAID_OVERPAYMENT
    ,NULL LAST_STMT_CLOSE_BALANCE
    ,NULL LAST_STMT_DATE
    ,NULL ACCOUNT_CCU
    ,'SUNPASS' SOURCE_SYSTEM
FROM  SUNPASS.ST_ACCT_LEDGER al
     ,PA_ACCT pa
WHERE pa.ACCT_NUM = al.ACCT_NUM;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_TPMS_ACCOUNT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in 1 .. DM_TPMS_ACCOUNT_INFO_tab.count loop

    /* get st_ledger.select nvl( max(posted_date),to_date('01/01/1900','mm/dd/yyyy')) negative from st_ledger for ST_BAL_NEG_DATE */
    begin
      select select nvl( max(posted_date),to_date('01/01/1900','mm/dd/yyyy')) negative  into DM_TPMS_ACCOUNT_INFO_tab(i).ST_BAL_NEG_DATE from st_ledger 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_BAL_NEG_DATE:=null;
    end;

    /* get st_ledger.select nvl( max(posted_date),to_date('01/01/1900','mm/dd/yyyy')) negative from st_ledger where  and acct_num = v_acct_num  for ST_BAL_POS_DATE */
    begin
      select select nvl( max(posted_date),to_date('01/01/1900','mm/dd/yyyy')) negative  into DM_TPMS_ACCOUNT_INFO_tab(i).ST_BAL_POS_DATE from st_ledger 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID and  balance<0
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_BAL_POS_DATE:=null;
    end;

    /* get st_ledger.GET ST_LEDGER.BALANCE where MAX(POSTED_DATE) <= (END OF MONTH DAY YEAR) for ST_STMT_CLOSE_BALANCE */
    begin
      select ST_LEDGER.BALANCE  into DM_TPMS_ACCOUNT_INFO_tab(i).ST_STMT_CLOSE_BALANCE from st_ledger 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and where MAX(POSTED_DATE) <= (END OF MONTH DAY YEAR)
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_STMT_CLOSE_BALANCE:=null;
    end;

    /* get ST_ACCT_LEDGER .ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion) for ST_FIN_TX_AMT */
    begin
      select ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion) into DM_TPMS_ACCOUNT_INFO_tab(i).ST_FIN_TX_AMT from ST_ACCT_LEDGER  
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_FIN_TX_AMT:=null;
    end;

    /* get ST_ACCT_LEDGER.ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_PUR_DET_ID) for ST_FIN_TX_ID */
    begin
      select ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_PUR_DET_ID) into DM_TPMS_ACCOUNT_INFO_tab(i).ST_FIN_TX_ID from ST_ACCT_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_FIN_TX_ID:=null;
    end;

    /* get ACCOUNTS_TO_COLLECTION.IF EXISTS IN THIS TABLE ACCOUNTS_TO_COLLECTION THEN 1 ELSE 0 for COUNT_CCU */
    begin
      select decode(sum(1),0,1) into DM_TPMS_ACCOUNT_INFO_tab(i).COUNT_CCU from ACCOUNTS_TO_COLLECTION 
      where ACTNUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).COUNT_CCU:=null;
    end;

    /* get ST_LEDGER.ST_SUB_LEDGER_EVENTS and ST_LEDGER and PRODUCT_CODE = 31 returns MAX(POSTED_DATE) EVENT_ID=id and ST_LEDGER.ACCT_NUM=ACCOUNT_NUMBER for ST_PAYMENT_DATE */
    begin
      select ST_SUB_LEDGER_EVENTS and ST_LEDGER and PRODUCT_CODE = 31 returns MAX(POSTED_DATE) EVENT_ID=id and ST_LEDGER.ACCT_NUM=ACCOUNT_NUMBER into DM_TPMS_ACCOUNT_INFO_tab(i).ST_PAYMENT_DATE from ST_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_PAYMENT_DATE:=null;
    end;

    /* get ST_LEDGER.ST_SUB_LEDGER_EVENTS and ST_LEDGER and PRODUCT_CODE = 31 returns MAX(POSTED_DATE) EVENT_ID=id and ST_LEDGER.ACCT_NUM=ACCOUNT_NUMBER for ST_PAYMENT_DATE */
    begin
      select ST_SUB_LEDGER_EVENTS and ST_LEDGER and PRODUCT_CODE = 31 returns MAX(POSTED_DATE) EVENT_ID=id and ST_LEDGER.ACCT_NUM=ACCOUNT_NUMBER into DM_TPMS_ACCOUNT_INFO_tab(i).ST_PAYMENT_DATE from ST_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_PAYMENT_DATE:=null;
    end;

    /* get ST_ACCT_LEDGER.ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_LANE_TXN_ID) and POSTED_DATE  for ST_TOLL_POSTED_DATE */
    begin
      select ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_LANE_TXN_ID) and POSTED_DATE  into DM_TPMS_ACCOUNT_INFO_tab(i).ST_TOLL_POSTED_DATE from ST_ACCT_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_TOLL_POSTED_DATE:=null;
    end;

    /* get ST_ACCT_LEDGER.ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_LANE_TXN_ID) and AMOUNT for ST_TOLL_TX_AMT */
    begin
      select ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_LANE_TXN_ID) and AMOUNT into DM_TPMS_ACCOUNT_INFO_tab(i).ST_TOLL_TX_AMT from ST_ACCT_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_TOLL_TX_AMT:=null;
    end;

    /* get ST_ACCT_LEDGER.ST_ACCT_LEDGER AND ST_LEDGER to PA_LANE_TXN returns MIN(EXT_DATE_TIME)  for ST_TOLL_TX_TIMESTAMP */
    begin
      select ST_ACCT_LEDGER AND ST_LEDGER to PA_LANE_TXN returns MIN(EXT_DATE_TIME)  into DM_TPMS_ACCOUNT_INFO_tab(i).ST_TOLL_TX_TIMESTAMP from ST_ACCT_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_TOLL_TX_TIMESTAMP:=null;
    end;

    /* get ST_ACCT_LEDGER .ST_ACCT_LEDGER AND ST_LEDGER to PA_LANE_TXN returns MIN(EXT_DATE_TIME)  for RST_TOLL_TIMESTAMP */
    begin
      select ST_ACCT_LEDGER AND ST_LEDGER to PA_LANE_TXN returns MIN(EXT_DATE_TIME)  into DM_TPMS_ACCOUNT_INFO_tab(i).RST_TOLL_TIMESTAMP from ST_ACCT_LEDGER  
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).RST_TOLL_TIMESTAMP:=null;
    end;

    /* get ST_ACCT_LEDGER.ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_LANE_TXN_ID) and AMOUNT for ST_TOLL_TX_ID */
    begin
      select ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_LANE_TXN_ID) and AMOUNT into DM_TPMS_ACCOUNT_INFO_tab(i).ST_TOLL_TX_ID from ST_ACCT_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_TOLL_TX_ID:=null;
    end;

    /* get ST_ACCT_LEDGER.ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion) for ST_FIN_TX_DATE */
    begin
      select ST_ACCT_LEDGER AND ST_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion) into DM_TPMS_ACCOUNT_INFO_tab(i).ST_FIN_TX_DATE from ST_ACCT_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ST_FIN_TX_DATE:=null;
    end;

    end loop;


	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_TPMS_ACCOUNT_INFO_tab.count loop
	 if DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).AGENCY_ID is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).AGENCY_ID:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_TYPE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_TYPE:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).CURRENT_BALANCE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).CURRENT_BALANCE:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_BALANCE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_BALANCE:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).DEVICE_DEPOSIT is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).DEVICE_DEPOSIT:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_PREPAYMENT is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_PREPAYMENT:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_OVERPAYMENT is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_OVERPAYMENT:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_POSTED_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_POSTED_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_AMT is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_AMT:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_TIMESTAMP is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_TIMESTAMP:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).FIRST_TOLL_TIMESTAMP is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).FIRST_TOLL_TIMESTAMP:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).EVAL_START_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).EVAL_START_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).EVAL_END_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).EVAL_END_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).UPDATE_TS is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).UPDATE_TS:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).OPEN_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).OPEN_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_UNBILLED_BALANCE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_UNBILLED_BALANCE:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_TOLL_BALANCE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_TOLL_BALANCE:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_FEE_BALANCE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_FEE_BALANCE:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_BALANCE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).VIOL_BALANCE:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_FIN_TX_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_FIN_TX_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_PAYMENT_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_PAYMENT_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_FIN_TX_AMT is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_FIN_TX_AMT:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_BAL_POS_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_BAL_POS_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_BAL_NEG_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_BAL_NEG_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_VIOL_PAYMENT_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_VIOL_PAYMENT_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_OVERPAYMENT is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_OVERPAYMENT:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_CLOSE_BALANCE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_CLOSE_BALANCE:='0';
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_DATE is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_DATE:=sysdate;
         end if;
	 if DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_CCU is null then
          DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_CCU:='0';
         end if;
    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_TPMS_ACCOUNT_INFO_tab.first .. DM_TPMS_ACCOUNT_INFO_tab.last
           INSERT INTO DM_TPMS_ACCOUNT_INFO VALUES DM_TPMS_ACCOUNT_INFO_tab(i);
                       
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


