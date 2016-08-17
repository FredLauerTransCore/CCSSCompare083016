/********************************************************
*
* Name: DM_ACCOUNT_INFO_PROC
* Created by: DT, 4/21/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCOUNT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on


CREATE OR REPLACE PROCEDURE DM_ACCOUNT_INFO_PROC IS



TYPE DM_ACCOUNT_INFO_TYP IS TABLE OF DM_ACCOUNT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_INFO_tab DM_ACCOUNT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_NUM ACCOUNT_NUMBER
    ,decode(ACCTSTAT_ACCT_STATUS_CODE,
     '01','Active',
     '02','Suspended',
     '03','Terminated',
     '04','Closed',
     '99','Do Not Use',
    ACCTSTAT_ACCT_STATUS_CODE||'NO-MAPPING'
    ) ACCOUNT_STATUS
    ,NULL ACCOUNT_STATUS_DATETIME
    ,'NONE' STATEMENT_DELIVERY_MODE
    ,'NONE' STATEMENT_PERIOD
    ,decode(ACCTTYPE_ACCT_TYPE_CODE,
     '01','PRIVATE',
     '02','BUSINESS',
     '03','PVIDEOREG',
     '04','PVIDEOUNREG',
     '05','SHORTTERMVIDEO',
     '06','BVIDEOREG',
     '07','BVIDEOUNREG',
     '08','PVIOLATOR',
     '09','CVIOLATOR',
     '10','10-NOMAPPING',
     '11','11-NOMAPPING',
     '12','12-NOMAPPING',
    ACCTTYPE_ACCT_TYPE_CODE||'NO-MAPPING'
    )  ACCOUNT_TYPE
    ,ACCT_OPEN_DATE ACCOUNT_OPEN_DATE
    ,F_NAME||' '||L_NAME ACCOUNT_NAME
    ,ORG COMPANY_NAME
    ,NULL DBA
    ,E_MAIL_ADDR EMAIL_ADDRESS
    ,REPL_AMT REBILL_AMOUNT
    ,REPL_THRESH_AMT REBILL_THRESHOLD
    ,decode(translate(lpad(ACCT_PIN_NUMBER,4,'9'),'0123456789','9999999999'),'9999',ACCT_PIN_NUMBER,null)
     PIN
    ,NULL VIDEO_ACCT_STATUS
    ,NULL SUSPENDED_DATE
    ,NULL LAST_INVOICE_DATE
    ,ACCT_CLOSE_DATE ACCOUNT_CLOSE_DATE
    ,NULL CORRESPONDENCE_DEL_MODE
    ,'ENGLISH' LANGUAGE_PREFERENCE
    ,'OPT-OUT' MOBILE_ALERTS
    ,NULL CHALLENGE_QUESTION
    ,NULL CHALLENGE_ANSWER
    ,NULL IS_POSTPAID
    ,'Y' IS_SURVEY_OPTED
    ,NULL ACC_LOCKED_DATETIME
    ,NULL ACCOUNT_LOCKED
    ,null NSF_LOCK_RELEASED_DT
    ,null CHARGEBACK_RELEASED_DT
    ,NULL BANKRUPTCY_DATE
    ,'N' COURT_FLAG
    ,NULL OUTBOUND_DAILER
    ,NULL DO_NOT_CALL
    ,NULL APPLICATION_NUMBER
    ,NULL COLLECTION_AGENCY
    ,NULL ACCOUNT_CLOSE_PEND_DT
    ,NULL DECEASED
    ,ACCT_OPEN_DATE CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,to_date('02/27/2017','MM/DD/YYYY') LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,MAIDEN_NAME MOTHERS_MAIDEN_NAME
    ,null FEIN_NUMBER
    ,NULL CAST_ACCOUNT_FLAG
    ,NULL TAX_EXEMPTION_ID
    ,NULL EXPIRATION_DATE
    ,ANON_ACCT ANONYMOUS_ACCOUNT
    ,NULL SECONDARY_EMAIL_ADDRESS
    ,'SUNPASS' SOURCE_SYSTEM
    ,NULL BKTY_FILE_DATE
    ,NULL BKTY_DICHG_DATE
    ,NULL BKTY_DISMISS_DATE
    ,NULL BKTY_CASEID
    ,NULL BKTY_NOTIFY_BY
    ,NULL IS_REGISTERED
    ,NULL OWNER_TYPE
   ,NULL DEATH_CERT_DATE
   ,NULL COLL_ASSIGN_DATE
   ,NULL DECEASE_NOTIFY_BY
   ,NULL INIT_COLL_ASSIGN_DATE
FROM PA_ACCT;
BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */



    FOR i in 1 .. DM_ACCOUNT_INFO_tab.count loop

    /* get PA_ACCT_STATUS_HISTORY.STAT_POST_DATE for ACCOUNT_STATUS_DATETIME */
    begin
      select STAT_POST_DATE into DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME 
      from PA_ACCT_STATUS_HISTORY 
      where ACCT_NUM=DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME:=null;
    end;


    /* get PA_ACCT_FLAGS.FEIN for FEIN_NUMBER */
    begin
      select 
      decode(translate(lpad(FEIN,12,'9'),'0123456789','9999999999'),'999999999999',FEIN,null),
      CAST
      into DM_ACCOUNT_INFO_tab(i).FEIN_NUMBER ,
      DM_ACCOUNT_INFO_tab(i).CAST_ACCOUNT_FLAG from PA_ACCT_FLAGS 
      where ACCT_ACCT_NUM=DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_INFO_tab(i).FEIN_NUMBER:=null;
    end;

	    /* get PA_COLLECTION_EXCEPTION.START_DATE for BANKRUPTCY_DATE */
    begin
      select START_DATE into DM_ACCOUNT_INFO_tab(i).BANKRUPTCY_DATE from PA_COLLECTION_EXCEPTION 
      where ACCT_NUM=DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_INFO_tab(i).BANKRUPTCY_DATE:=null;
    end;

    /* get PA_ACCT_TAX_EXEMPTIONS.TAX_EXEMPTION_ID for TAX_EXEMPTION_ID */
    begin
      select TAX_EXEMPTION_ID,EXPIRATION_DATE  into 
      DM_ACCOUNT_INFO_tab(i).TAX_EXEMPTION_ID,
      DM_ACCOUNT_INFO_tab(i).EXPIRATION_DATE  from PA_ACCT_TAX_EXEMPTIONS 
      where ACCT_ACCT_NUM=DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCOUNT_INFO_tab(i).TAX_EXEMPTION_ID:=null;
    end;

    null;

    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_INFO_tab.first .. DM_ACCOUNT_INFO_tab.last
           INSERT INTO DM_ACCOUNT_INFO VALUES DM_ACCOUNT_INFO_tab(i);
                       
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



