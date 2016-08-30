/********************************************************
*
* Name: DM_TPMS_ACCOUNT_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_TPMS_ACCOUNT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_TPMS_ACCOUNT_INFO_PROC   
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_TPMS_ACCOUNT_INFO_TYP IS TABLE OF DM_TPMS_ACCOUNT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_TPMS_ACCOUNT_INFO_tab DM_TPMS_ACCOUNT_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    ACCT_NUM ETC_ACCOUNT_ID  -- DERIVED?  from ACCOUNT_NO 
    ,0 AGENCY_ID   -- CCSS
    ,NULL ACCOUNT_TYPE  -- in ETL PA_ACCT.ACCTTYPE_ACCT_TYPE_CODE

--(va.AMT_CHARGED - va.TOTAL_AMT_PAID)    ?
---- Anything with a positive balance is considered prepaid, 
--    ,case when CUST_BALANCE > 0 then CUST_BALANCE
--     else 0
--     end CURRENT_BALANCE  
    ,CUST_BALANCE CURRENT_BALANCE  

-- anything with a negative balance is considered postpaid.
    ,case when CUST_BALANCE < 0 then CUST_BALANCE
     else 0
     end POSTPAID_BALANCE  
    
    ,0 DEVICE_DEPOSIT
    ,0 VIOL_PREPAYMENT
    ,0 VIOL_OVERPAYMENT
    ,'N' IS_DISCOUNT_ELIGIBLE
    
------- DERIVED in ETL - KS_LEDGER
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_LANE_TXN_ID)
    ,NULL LAST_TOLL_TX_ID 
    
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_LANE_TXN_ID) and POSTED_DATE
    ,NULL LAST_TOLL_POSTED_DATE

-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_LANE_TXN_ID) and AMOUNT
    ,NULL LAST_TOLL_TX_AMT

--KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns EXT_DATE_TIME (Time Portion)
    ,NULL LAST_TOLL_TX_TIMESTAMP
    
-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns MIN(EXT_DATE_TIME) 
-- ** Based on available Activity, not actual first toll in the system for account (Time Portion)
    ,NULL FIRST_TOLL_TIMESTAMP

-- Xerox will provide during discussion
    ,to_date('01-01-1900','MM-DD-YYYY') EVAL_START_DATE
    ,to_date('01-01-1900','MM-DD-YYYY') EVAL_END_DATE
    
    ,0 USAGE_AMOUNT
    ,LAST_UPDATED UPDATE_TS
    ,NULL OPEN_DATE     --  PA_ACCT.ACCT_OPEN_DATE
    ,0 POSTPAID_UNBILLED_BALANCE
    ,0 VIOL_TOLL_BALANCE
    ,0 VIOL_FEE_BALANCE
    ,0 VIOL_BALANCE
        
-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns EXT_DATE_TIME (Date Portion)
    ,NULL LAST_TOLL_TX_DATE
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion)
    ,NULL LAST_FIN_TX_DATE
    
    ,0 ACCT_FIN_STATUS

-- KS_SUB_LEDGER_EVENTS and KS_LEDGER and GROUP_CODE = 1 
-- returns MAX(LEDGER_ID) WHERE EVENT_TYPE_ID IN 
-- (13, 56, 40, 26, 10, 57, 66, 115, 29, 14, 15, 67, 96)    
    ,NULL LAST_PAYMENT_TX_ID

-- POSTED_DATE OF KS_SUB_LEDGER and KS_LEDGER and GROUP_CODE = 1 
-- returns MAX(LEDGER_ID) WHERE EVENT_TYPE_ID IN  
--(13, 56, 40, 26, 10, 57, 66, 115, 29, 14, 15, 67, 96)    
    ,NULL LAST_PAYMENT_DATE

-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID)
    ,NULL LAST_FIN_TX_ID
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion)
    ,NULL LAST_FIN_TX_AMT

--Xerox - when staging data is available this will be populated    
    ,to_date('01-01-1900','MM-DD-YYYY') LAST_BAL_POS_DATE
    ,to_date('01-01-1900','MM-DD-YYYY') LAST_BAL_NEG_DATE
    ,to_date('01-01-1900','MM-DD-YYYY') LAST_VIOL_PAYMENT_DATE
    ,0 POSTPAID_OVERPAYMENT

-- KS_LEDGER  BALANCE where MAX(POSTED_DATE) <= (END OF MONTH DAY YEAR)
    ,NULL LAST_STMT_CLOSE_BALANCE
-- KS_LEDGER  POSTED_DATE where MAX(POSTED_DATE) <= (END OF MONTH DAY YEAR)
    ,NULL LAST_STMT_DATE
    
    ,NULL ACCOUNT_CCU       -- PA_ACCT_DETAIL.CA_AGENCY_ID
    ,'SUNTOLL' SOURCE_SYSTEM
    ,LEDGER_ID LEDGER_ID
    ,0 POSTPAID_FEE
    ,0 COLL_TOLL
    ,0 COLL_FEE
    ,0 UTC_TOLL
    ,0 UTC_FEE
    ,0 COURT_TOLL
    ,0 COURT_FEE
FROM KS_ACCT_LEDGER kal
WHERE ACCT_NUM >= p_begin_acct_num AND ACCT_NUM <= p_end_acct_num
and ACCT_NUM >0
; 

TYPE event_id_typ IS TABLE OF NUMBER;
event_type_list event_id_typ;

row_cnt          NUMBER := 0;
v_trac_rec       dm_tracking%ROWTYPE;
v_trac_etl_rec   dm_tracking_etl%ROWTYPE;

BEGIN
  SELECT * INTO v_trac_etl_rec
  FROM  dm_tracking_etl
  WHERE track_etl_id = i_trac_id;
  DBMS_OUTPUT.PUT_LINE('Start '||v_trac_etl_rec.etl_name||' ETL load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  v_trac_etl_rec.status := 'ETL Start ';
  v_trac_etl_rec.proc_start_date := SYSDATE;  
  update_track_proc(v_trac_etl_rec);
 
  SELECT * INTO   v_trac_rec
  FROM   dm_tracking
  WHERE  track_id = v_trac_etl_rec.track_id
  ;

  OPEN C1(v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_TPMS_ACCOUNT_INFO_tab
    LIMIT P_ARRAY_SIZE;

    /* ETL SECTION BEGIN*/
    FOR i IN 1 .. DM_TPMS_ACCOUNT_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID;
      end if;

-- DERIVED in ETL KS_ACCT_LEDGER AND KS_LEDGER 
-- returns MAX(PA_LANE_TXN_ID)
-- and POSTED_DATE and AMOUNT
-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns EXT_DATE_TIME (Time Portion)
-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns MIN(EXT_DATE_TIME) 
-- ** Based on available Activity, not actual first toll in the system for account (Time Portion)
--    ,NULL FIRST_TOLL_TIMESTAMP


--  KS_ACCT_LEDGER AND KS_LEDGER 
--  returns MAX(PA_LANE_TXN_ID) POSTED_DATE and AMOUNT        

      begin
        select PA_LANE_TXN_ID
              ,trunc(POSTED_DATE)
              ,AMOUNT
        into  DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_ID
              ,DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_POSTED_DATE
              ,DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_AMT
        from  KS_LEDGER
        where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
           and PA_LANE_TXN_ID = (select max(PA_LANE_TXN_ID)
                                  from KS_LEDGER
                                  where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
                                  and PA_LANE_TXN_ID>0 
                                  )
            and rownum<=1
        ;
      exception 
        when others then null;
             dm_tpms_account_Info_tab(i).LAST_TOLL_POSTED_DATE:=null; 
             dm_tpms_account_Info_tab(i).LAST_TOLL_TX_ID:=null;
             dm_tpms_account_Info_tab(i).LAST_TOLL_TX_AMT:=0;
      end;


-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns MIN(EXT_DATE_TIME) 
-- ** Based on available Activity, not actual first toll in the system for account (Time Portion)
--    FIRST_TOLL_TIMESTAMP

      begin
        select EXT_DATE_TIME
        into  DM_TPMS_ACCOUNT_INFO_tab(i).FIRST_TOLL_TIMESTAMP
        from  PA_LANE_TXN
        where TXN_ID = (select min(PA_LANE_TXN_ID)
                                  from KS_LEDGER
                                  where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
                                  and PA_LANE_TXN_ID>0 
                                  )
            and rownum<=1
        ;
      exception 
        when others then null;
             dm_tpms_account_Info_tab(i).FIRST_TOLL_TIMESTAMP:=null; 
      end;


-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns EXT_DATE_TIME (Time Portion)
--    LAST_TOLL_TX_AMT
-- KS_ACCT_LEDGER AND KS_LEDGER to PA_LANE_TXN returns EXT_DATE_TIME (Date Portion)
--    LAST_TOLL_TX_DATE

    begin
      select EXT_DATE_TIME
            ,trunc(EXT_DATE_TIME)  
      into  dm_tpms_account_info_tab(i).LAST_TOLL_TX_TIMESTAMP
            ,dm_tpms_account_info_tab(i).LAST_TOLL_TX_DATE 
      from  PA_LANE_TXN 
      where TXN_ID=(select max(PA_LANE_TXN_ID) from KS_LEDGER
                                where ACCT_NUM=dm_tpms_account_Info_tab(i).ETC_ACCOUNT_ID 
                                and PA_LANE_TXN_ID>0 
                    )
            and rownum<=1
            ;
      exception 
        when others then null;
        dm_tpms_account_info_tab(i).LAST_TOLL_TX_TIMESTAMP:=null;
    end;


-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion)
--    LAST_FIN_TX_DATE
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID)
--    LAST_FIN_TX_ID
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion)
--    LAST_FIN_TX_AMT

-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID)
-- KS_ACCT_LEDGER AND KS_LEDGER returns MAX(PA_PUR_DET_ID) and POSTED_DATE (Date Portion)
      begin
        select PA_PUR_DET_ID
              ,AMOUNT
        into  DM_TPMS_ACCOUNT_INFO_tab(i).LAST_FIN_TX_ID
              ,DM_TPMS_ACCOUNT_INFO_tab(i).LAST_FIN_TX_AMT
        from  KS_LEDGER
        where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
           and ID = DM_TPMS_ACCOUNT_INFO_tab(i).LEDGER_ID
           and PA_PUR_DET_ID = (select max(PA_PUR_DET_ID)
                                  from KS_LEDGER
                                  where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID)
        ;
      exception 
        when no_data_found then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_ID:=0;       
      end;



------ KS_SUB_LEDGER_EVENTS and KS_LEDGER and GROUP_CODE = 1 
------ returns MAX(LEDGER_ID) WHERE EVENT_TYPE_ID IN 
------ (13, 56, 40, 26, 10, 57, 66, 115, 29, 14, 15, 67, 96)    
----    ,NULL LAST_PAYMENT_TX_ID
      begin
        select MAX(kl.ID)
        into  DM_TPMS_ACCOUNT_INFO_tab(i).LAST_PAYMENT_TX_ID
        from  KS_LEDGER kl  --
              ,KS_SUB_LEDGER_EVENTS ksle
        where ksle.COURT_ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
        and   kl.ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
        and   kl.transaction_type = ksle.event_id
        and   ksle.EVENT_TYPE_ID IN (13, 56, 40, 26, 10, 57, 66, 115, 29, 14, 15, 67, 96) 
        and   ksle.EVENT_GROUP_ID = 1
        ;

--        WHERE kl.acct_num = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID -- in (select acct_num from pa_acct) 
----        and kl.id = DM_TPMS_ACCOUNT_INFO_tab(i).LEDGER_ID 
--        and kl.transaction_type = ksle.event_id

--        select KS_SUB_LEDGER_EVENTS
--        into  DM_TPMS_ACCOUNT_INFO_tab(i).LAST_PAYMENT_TX_ID
--        from  KS_SUB_LEDGER_EVENTS
--        where COURT_ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
--        and   EVENT_TYPE_ID IN (13, 56, 40, 26, 10, 57, 66, 115, 29, 14, 15, 67, 96) 
--        and   EVENT_GROUP_ID = 1
--        ;
      exception 
        when others then
          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_PAYMENT_TX_ID:=0;
      end; 
      
-------- POSTED_DATE OF KS_SUB_LEDGER and KS_LEDGER and GROUP_CODE = 1 
-------- returns MAX(LEDGER_ID) WHERE EVENT_TYPE_ID IN  
--------(13, 56, 40, 26, 10, 57, 66, 115, 29, 14, 15, 67, 96)    
------    ,NULL LAST_PAYMENT_DATE
--      begin
--        select POSTED_DATE
--        into  DM_TPMS_ACCOUNT_INFO_tab(i).LAST_PAYMENT_DATE
--        from  KS_SUB_LEDGER
--        where COURT_ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
--        and   EVENT_TYPE_ID IN (13, 56, 40, 26, 10, 57, 66, 115, 29, 14, 15, 67, 96) 
--        and   EVENT_GROUP_ID = 1
--        ;
--      exception 
--        when others then
--          DM_TPMS_ACCOUNT_INFO_tab(i).LAST_TOLL_TX_ID:=0;
--      end;    

-- KS_LEDGER  BALANCE where MAX(POSTED_DATE) <= (END OF MONTH DAY YEAR)
--    LAST_STMT_CLOSE_BALANCE
-- KS_LEDGER POSTED_DATE where MAX(POSTED_DATE) <= (END OF MONTH DAY YEAR)
--    LAST_STMT_DATE
    begin
      select POSTED_DATE
            ,BALANCE
      into  DM_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_DATE
            ,DM_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_CLOSE_BALANCE
      from  KS_LEDGER 
      where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
        and posted_date=(select max(posted_date) 
                             from  KS_LEDGER 
                             where ACCT_NUM=DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID 
                             and posted_date<=last_day(sysdate)
                             )
            and rownum<=1;
      exception 
        when others then null;
        dm_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_DATE:=null;
        dm_TPMS_ACCOUNT_INFO_tab(i).LAST_STMT_DATE:=null;
      end;
      
      
--   ACCOUNT_TYPE  -- PA_ACCT.ACCTTYPE_ACCT_TYPE_CODE
--   OPEN_DATE     -- PA_ACCT.ACCT_OPEN_DATE
      begin
        select ACCTTYPE_ACCT_TYPE_CODE
              ,CREATED_ON
        into  DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_TYPE
              ,DM_TPMS_ACCOUNT_INFO_tab(i).OPEN_DATE
        from PA_ACCT
        where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
        ;
      exception 
        when others then null;
          DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_TYPE:=0;
          DM_TPMS_ACCOUNT_INFO_tab(i).OPEN_DATE:=to_date('01/01/1900','MM/DD/YYYY');
      end;

--  ACCOUNT_CCU --  PA_ACCT_DETAIL.CA_AGENCY_ID   
      begin
        select CA_AGENCY_ID
        into  DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_CCU
        from  PA_ACCT_DETAIL
        where ACCT_NUM = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID
        ;
      exception 
        when others then null;
          DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_CCU:=0;
       end;

--      begin
--CASE WHEN va.BANKRUPTCY_FLAG = 'Y' and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0  THEN 'BANKRUPTCY_TOLL'
--WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0  THEN 'CUST_TOLL'
--WHEN va.COLL_COURT_FLAG = 'COLL' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 THEN 'COLLECTION_TOLL'
--WHEN va.COLL_COURT_FLAG = 'INTP' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 THEN 'INTEROP_TOLL-CCSS EXCLUDED'
--WHEN va.COLL_COURT_FLAG = 'CRT' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0  THEN 'COURT_TOLL'
--WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0  THEN 'UTC_TOLL'
--ELSE  'OTHER' END) status,
--sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID) as amount
--from  KS_LEDGER kl,
--      VB_ACTIVITY va,
--      KS_SUB_LEDGER_EVENTS ksle
--WHERE kl.acct_num in (select acct_num from pa_acct) 
--and kl.id = va.ledger_id 
--and kl.transaction_type = ksle.event_id
--and ksle.event_type_id in (89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)
--and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0
--;
--      exception 
--        when others then null;
--          DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_CCU:=0;
--       end;

--'CUST_TOLL'	 POSTPAID_BALANCE
--'CUST_NON_TOLL'	 POSTPAID_FEE
--'COLLECTION_TOLL'	COLL_TOLL
--'COLLECTION_NON_TOLL'	COLL_FEE
--'INTEROP_TOLL-CCSS EXCLUDED'	 
--'INTEROP_NON_TOLL-CCSS EXCLUDED'	 
--'COURT_TOLL'	COURT_TOLL
--'COURT_NON_TOLL'	COURT_FEE
--'UTC_TOLL'	UTC_TOLL
--'UTC_NON_TOLL'	UTC_FEE

--    ,0 POSTPAID_BALANCE   'CUST_TOLL'  mapped above 

--    ,0 POSTPAID_FEE   'CUST_NON_TOLL'
--    ,0 COLL_TOLL      'COLLECTION_TOLL'
--    ,0 COLL_FEE       'COLLECTION_NON_TOLL'
--    ,0 UTC_TOLL       'UTC_TOLL'
--    ,0 UTC_FEE        'UTC_NON_TOLL'
--    ,0 COURT_TOLL
--    ,0 COURT_FEE      'COURT_NON_TOLL'
 
-- (select * from table(MyList));   
-- (select * from table(event_type_list));   
      event_type_list := event_id_typ(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4) ;

      begin
        select 

----WHEN va.BANKRUPTCY_FLAG = 'Y' and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)   THEN 'BANKRUPTCY_TOLL'
----WHEN va.BANKRUPTCY_FLAG = 'Y' and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)   THEN 'BANKRUPTCY_NON_TOLL'
--
--         CASE WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null -- and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
----  and ksle.event_type_id MEMBER of event_itype_list
----  and ksle.event_type_id in (select event_type_id from table(event_type_list))
--    and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  
--              THEN sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID)  -- POSTPAID_BALANCE   'CUST_TOLL'
--              ELSE 0
--          END 
--          
--          ,
          CASE WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null -- and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                      and ksle.event_type_id not in (select * from table(event_type_list))
                and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  
                THEN sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID)  -- POSTPAID_FEE 'CUST_NON_TOLL'
                ELSE 0
          END
          
--WHEN va.COLL_COURT_FLAG = 'COLL' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'COLLECTION_TOLL'
          ,CASE WHEN va.COLL_COURT_FLAG = 'COLL' and va.BANKRUPTCY_FLAG is null -- and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--              and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                      and ksle.event_type_id in (select * from table(event_type_list))
                and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  
                THEN sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID)  -- COLL_TOLL      'COLLECTION_TOLL'
                ELSE 0
          END
          
--WHEN va.COLL_COURT_FLAG = 'COLL' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'COLLECTION_NON_TOLL'
          ,CASE WHEN va.COLL_COURT_FLAG = 'COLL' and va.BANKRUPTCY_FLAG is null 
--                and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                      and ksle.event_type_id not in (select * from table(event_type_list))
                and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  
                THEN sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID)  -- COLL_FEE       'COLLECTION_NON_TOLL'
                ELSE 0
          END

----WHEN va.COLL_COURT_FLAG = 'INTP' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'INTEROP_TOLL-CCSS EXCLUDED'
----WHEN va.COLL_COURT_FLAG = 'INTP' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'INTEROP_NON_TOLL-CCSS EXCLUDED'

--WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and va.CHILD_DOC_ID like '%-%' and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'UTC_TOLL'
          ,CASE WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null -- and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
                and va.CHILD_DOC_ID like '%-%'
--                      and ksle.event_type_id in (select * from table(event_type_list))
                and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  
                THEN sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID)  -- UTC_TOLL       'UTC_TOLL'
                ELSE 0
          END

--WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and va.DOCUMENT_ID like '%-%' and va.CHILD_DOC_ID is null and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4) THEN 'UTC_NON_TOLL'
          ,CASE WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null -- and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
                and va.DOCUMENT_ID like '%-%' and va.CHILD_DOC_ID is null
--                      and ksle.event_type_id not in (select * from table(event_type_list))
                and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  
                THEN sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID)  -- UTC_FEE        'UTC_NON_TOLL'
                ELSE 0
          END

--WHEN va.COLL_COURT_FLAG = 'CRT' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'COURT_TOLL'
          ,CASE WHEN va.COLL_COURT_FLAG = 'CRT' and va.BANKRUPTCY_FLAG is null -- and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                      and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                      and ksle.event_type_id in (select * from table(event_type_list))
                      and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  
                THEN sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID)  -- COURT_TOLL 
                ELSE 0
          END

--WHEN va.COLL_COURT_FLAG = 'CRT' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'COURT_NON_TOLL'
          ,CASE WHEN va.COLL_COURT_FLAG = 'CRT' and va.BANKRUPTCY_FLAG is null -- and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                      and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--                      and ksle.event_type_id not in (select * from table(event_type_list))
                and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  
                THEN sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID)  -- COURT_FEE      'COURT_NON_TOLL'
                ELSE 0
          END

--ELSE  'OTHER' END)  -- status,sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID) as amount

        into  --DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_BALANCE,
              DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_FEE
              ,DM_TPMS_ACCOUNT_INFO_tab(i).COLL_TOLL
              ,DM_TPMS_ACCOUNT_INFO_tab(i).COLL_FEE
              ,DM_TPMS_ACCOUNT_INFO_tab(i).UTC_TOLL
              ,DM_TPMS_ACCOUNT_INFO_tab(i).UTC_FEE
              ,DM_TPMS_ACCOUNT_INFO_tab(i).COURT_TOLL
              ,DM_TPMS_ACCOUNT_INFO_tab(i).COURT_FEE
        from  KS_LEDGER kl,
              VB_ACTIVITY va,
              KS_SUB_LEDGER_EVENTS ksle
        WHERE kl.acct_num = DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID -- in (select acct_num from pa_acct) 
          and kl.id = va.LEDGER_ID
--        and kl.id = DM_TPMS_ACCOUNT_INFO_tab(i).LEDGER_ID 
--        and va.LEDGER_ID = DM_TPMS_ACCOUNT_INFO_tab(i).LEDGER_ID 
          and kl.transaction_type = ksle.event_id
--        and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 
--and ksle.event_type_id in not event_type_list
        ;
      exception 
        when others then null;
--          DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_BALANCE :=0;
          DM_TPMS_ACCOUNT_INFO_tab(i).POSTPAID_FEE :=0;
          DM_TPMS_ACCOUNT_INFO_tab(i).COLL_TOLL :=0;
          DM_TPMS_ACCOUNT_INFO_tab(i).COLL_FEE :=0;
      end;

--      begin
--CASE 
--WHEN va.BANKRUPTCY_FLAG = 'Y' and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)   THEN 'BANKRUPTCY_TOLL'
--WHEN va.BANKRUPTCY_FLAG = 'Y' and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)   THEN 'BANKRUPTCY_NON_TOLL'
--WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'CUST_TOLL'
--WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'CUST_NON_TOLL'
--WHEN va.COLL_COURT_FLAG = 'COLL' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'COLLECTION_TOLL'
--WHEN va.COLL_COURT_FLAG = 'COLL' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'COLLECTION_NON_TOLL'
--WHEN va.COLL_COURT_FLAG = 'INTP' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'INTEROP_TOLL-CCSS EXCLUDED'
--WHEN va.COLL_COURT_FLAG = 'INTP' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'INTEROP_NON_TOLL-CCSS EXCLUDED'
--WHEN va.COLL_COURT_FLAG = 'CRT' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'COURT_TOLL'
--WHEN va.COLL_COURT_FLAG = 'CRT' and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'COURT_NON_TOLL'
--WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and va.CHILD_DOC_ID like '%-%' and ksle.event_type_id in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)  THEN 'UTC_TOLL'
--WHEN va.COLL_COURT_FLAG is null and va.BANKRUPTCY_FLAG is null and (va.AMT_CHARGED - va.TOTAL_AMT_PAID) >0 and va.DOCUMENT_ID like '%-%' and va.CHILD_DOC_ID is null and ksle.event_type_id not in(89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4) THEN 'UTC_NON_TOLL'
--ELSE  'OTHER' END) status,
--sum(va.AMT_CHARGED - va.TOTAL_AMT_PAID) as amount
--from  KS_LEDGER kl,
--      VB_ACTIVITY va,
--      KS_SUB_LEDGER_EVENTS ksle
--WHERE kl.acct_num in (select acct_num from pa_acct) 
--and kl.id = va.ledger_id 
--and kl.transaction_type = ksle.event_id
--and ksle.event_type_id in not (89,121,125,105,135,143,123,131,42,140,98,38,128,61,101,4)
--      exception 
--        when others then null;
--          DM_TPMS_ACCOUNT_INFO_tab(i).ACCOUNT_CCU:=0;
--       end;

-- IF NULLs
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

      v_trac_etl_rec.track_last_val := DM_TPMS_ACCOUNT_INFO_tab(i).ETC_ACCOUNT_ID;
    END LOOP;

    /* ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_TPMS_ACCOUNT_INFO_tab.first .. DM_TPMS_ACCOUNT_INFO_tab.last
           INSERT INTO DM_TPMS_ACCOUNT_INFO VALUES DM_TPMS_ACCOUNT_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    v_trac_etl_rec.end_val := v_trac_etl_rec.track_last_val;
    update_track_proc(v_trac_etl_rec);
    COMMIT;
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Total Load count : '||ROW_CNT);

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
  EXCEPTION
  WHEN OTHERS THEN
    v_trac_etl_rec.result_code := SQLCODE;
    v_trac_etl_rec.result_msg := SQLERRM;
    v_trac_etl_rec.proc_end_date := SYSDATE;
    update_track_proc(v_trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('KS_ACCT_LEDGER ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS

commit;

