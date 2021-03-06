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
    ,'CCSS' AGENCY_ID  --Is a number in target
    ,NULL ACCOUNT_TYPE
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
    ,NULL  OPEN_DATE
    ,'0' POSTPAID_UNBILLED_BALANCE
    ,'0' VIOL_TOLL_BALANCE
    ,'0' VIOL_FEE_BALANCE
    ,'0' VIOL_BALANCE
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
    ,al.LEDGER_ID LEDGER_ID
--    ,NULL POSTPAID_FEE
--    ,NULL COLL_TOLL
--    ,NULL COLL_FEE
--    ,NULL UTC_TOLL
--    ,NULL UTC_FEE
--    ,NULL COURT_TOLL
--    ,NULL COURT_FEE
FROM  ST_ACCT_LEDGER al;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_TPMS_ACCOUNT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in 1 .. DM_TPMS_ACCOUNT_INFO_tab.count loop
	

	
	    /* get PA_ACCT.ACCTTYPE_ACCT_TYPE_CODE for ACCOUNT_TYPE */
    begin
      select ACCTTYPE_ACCT_TYPE_CODE, ACCT_OPEN_DATE
      into 
      DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_TYPE, DM_TPMS_ACCOUNT_INFO_tab(i).OPEN_DATE
       from PA_ACCT 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_TYPE:=null;
    end;

	    /* get ST_LEDGER.POSTED_DATE for LAST_TOLL_POSTED_DATE */
    begin
      select POSTED_DATE, PA_LANE_TXN_ID, AMOUNT
             into 
             dm_tpms_account_Info_tab(i).LAST_TOLL_POSTED_DATE, 
             dm_tpms_account_Info_tab(i).LAST_TOLL_TX_ID,
             dm_tpms_account_Info_tab(i).LAST_TOLL_TX_AMT
      from ST_LEDGER 
      where ACCT_NUM=dm_tpms_account_Info_tab(i).ETC_ACCOUNT_ID
            and PA_LANE_TXN_ID=(select max(PA_LANE_TXN_ID) from ST_LEDGER
                                where ACCT_NUM=dm_tpms_account_Info_tab(i).ETC_ACCOUNT_ID and PA_LANE_TXN_ID>0 
                               )
            and rownum<=1;
      exception 
        when others then null;
             dm_tpms_account_Info_tab(i).LAST_TOLL_POSTED_DATE:=null; 
             dm_tpms_account_Info_tab(i).LAST_TOLL_TX_ID:=null;
             dm_tpms_account_Info_tab(i).LAST_TOLL_TX_AMT:=0;
    end;
	
	    /* get PA_LANE_TX.EXT_DATE_TIME for LAST_TOLL_TX_TIMESTAMP */
    begin
      select EXT_DATE_TIME,EXT_DATE_TIME  
	         into 
			 dm_tpms_account_info_tab(i).LAST_TOLL_TX_TIMESTAMP,
			 dm_tpms_account_info_tab(i).LAST_TOLL_TX_DATE 
	  from PA_LANE_TXN 
      where TXN_ID=(select max(PA_LANE_TXN_ID) from ST_LEDGER
                                where ACCT_NUM=dm_tpms_account_Info_tab(i).ETC_ACCOUNT_ID and PA_LANE_TXN_ID>0 
                    )
            and rownum<=1;
      exception 
        when others then null;
        dm_tpms_account_info_tab(i).LAST_TOLL_TX_TIMESTAMP:=null;
    end;

	
         /* get ST_LEDGER.POSTED_DATE for FIRST_TOLL_TIMESTAMP */
    begin
      select POSTED_DATE into DM_TPMS_ACCOUNT_INFO_tab(i).FIRST_TOLL_TIMESTAMP from ST_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
        and POSTED_DATE=(select min(POSTED_DATE) from ST_LEDGER where 
            ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID and PA_LANE_TXN_ID>0)
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).FIRST_TOLL_TIMESTAMP:=null;
    end;
	
	
		    /* get ST_LEDGER.POSTED_DATE for LAST_TOLL_POSTED_DATE */
    begin
      select POSTED_DATE, PA_PUR_DET_ID, AMOUNT
             into 
             dm_tpms_account_Info_tab(i).LAST_FIN_TX_DATE, 
             dm_tpms_account_Info_tab(i).LAST_FIN_TX_ID,
             dm_tpms_account_Info_tab(i).LAST_FIN_TX_AMT
      from ST_LEDGER 
      where ACCT_NUM=dm_tpms_account_Info_tab(i).ETC_ACCOUNT_ID
            and PA_PUR_DET_ID=(select max(PA_PUR_DET_ID) from ST_LEDGER
                                where ACCT_NUM=dm_tpms_account_Info_tab(i).ETC_ACCOUNT_ID and PA_PUR_DET_ID>0 
                               )
            and rownum<=1;
      exception 
        when others then null;
             dm_tpms_account_Info_tab(i).LAST_TOLL_POSTED_DATE:=null; 
             dm_tpms_account_Info_tab(i).LAST_TOLL_TX_ID:=null;
             dm_tpms_account_Info_tab(i).LAST_TOLL_TX_AMT:=0;
    end;
	
	

    /* get ST_LEDGER.POSTED_DATE for LAST_BAL_POS_DATE */
    begin
      select max(POSTED_DATE) into dm_TPMS_ACCOUNT_INFO_tab(i).LAST_BAL_POS_DATE from ST_LEDGER 
      where ACCT_NUM=dm_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID and balance>0
            and rownum<=1;
      exception 
        when others then null;
        dm_TPMS_ACCOUNT_INFO_tab(i).LAST_BAL_POS_DATE:=to_date('01/01/1900','MM/DD/YYYY');
    end;

    /* get ST_LEDGER.POSTED_DATE for LAST_BAL_NEG_DATE */
    begin
      select POSTED_DATE into dm_TPMS_ACCOUNT_INFO_tab(i).LAST_BAL_NEG_DATE from ST_LEDGER 
      where ACCT_NUM=dm_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID and balance<0
            and rownum<=1;
      exception 
        when others then null;
        dm_TPMS_ACCOUNT_INFO_tab(i).LAST_BAL_NEG_DATE:=to_date('01/01/1900','MM/DD/YYYY');
    end;

 
    /* get ST_LEDGER.POSTED_DATE, BALANCE */
    begin
      select POSTED_DATE,BALANCE
	  into 
	  dm_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_DATE,
	  dm_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_CLOSE_BALANCE
	  from ST_LEDGER 
      where ACCT_NUM=dm_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and posted_date=(select max(posted_date) from ST_LEDGER 
                             where ACCT_NUM=dm_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID 
                             and posted_date<=last_day(sysdate)
                             )
            and rownum<=1;
      exception 
        when others then null;
        dm_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_DATE:=null;
    end;
	
	
    /* get ACCOUNTS_TO_COLLECTION.1 for ACCOUNT_CCU */
    begin
      select sum(1) into DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_CCU from ACCOUNTS_TO_COLLECTION 
      where ACTNUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_CCU:=0;
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


