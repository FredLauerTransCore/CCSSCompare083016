/********************************************************
*
* Name: DM_ADDRESS_HST_INFO_PROC
* Created by: RH, 5/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ADDRESS_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- RH 6/20/2016 Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_ADDRESS_HST_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_ADDRESS_HST_INFO_TYP IS TABLE OF DM_ADDRESS_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ADDRESS_HST_INFO_tab DM_ADDRESS_HST_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT
    ACCT_NUM  ACCOUNT_NUMBER
    ,'MAILING' ADDR_TYPE
    ,1 ADDR_TYPE_INT_ID
    ,ADDR1 STREET_1 
    ,ADDR2 STREET_2
    ,CITY CITY
    ,STATE_CODE_ABBR STATE 
    ,ZIP_CODE ZIP_CODE
    ,SUBSTR(ZIP_CODE,7,10) ZIP_PLUS4
    ,COUNTRY_CODE COUNTRY 
    ,(select nvl(paf.MAIL_RETURNED,'N') from PATRON.PA_ACCT_FLAGS paf
        where paf.ACCT_ACCT_NUM = ACCT_NUM) 
        NIXIE -- PA_ACCT_FLAGS.RETURNED_MAIL_FLAG
    ,to_date('02/27/2017', 'MM/DD/YYYY') NIXIE_DATE
    ,'N' NCOA_FLAG
    ,'N' ADDRESS_CLEANSED_FLG
    ,'CSC' ADDRESS_SOURCE
--    ,CREATED_ON CREATED
    ,(select pa.CREATED_ON from PA_ACCT pa
          WHERE pa.ACCT_NUM = paa.ACCT_NUM)  CREATED
    ,'SUNTOLL_CSC_ID' CREATED_BY
    ,to_date('02/27/2017', 'MM/DD/YYYY') LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY
    ,COUNTY_CODE COUNTY_CODE
    ,'SUNTOLL' SOURCE_SYSTEM
--    ,NULL ADDRESS_NUMBER  -- DECODE?
    ,NULL ACTIVITY_ID
    ,0 VERSION
    ,NULL EMP_NUM
    ,NULL STATUS
FROM PA_ACCT_ADDR paa
--where 
--AND   paa.ACCT_NUM >= p_begin_acct_num AND   paa.ACCT_NUM <= p_end_acct_num
; -- Source
/*Change FTE_TABLE to the actual table name*/

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

  OPEN C1;   -- (v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ADDRESS_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ADDRESS_HST_INFO_tab.first .. DM_ADDRESS_HST_INFO_tab.last
           INSERT INTO DM_ADDRESS_HST_INFO VALUES DM_ADDRESS_HST_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total ROW_CNT : '||ROW_CNT);

  COMMIT;

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  
  EXCEPTION
  WHEN OTHERS THEN
    v_trac_etl_rec.result_code := SQLCODE;
    v_trac_etl_rec.result_msg := SQLERRM;
    v_trac_etl_rec.proc_end_date := SYSDATE;
    update_track_proc(v_trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS




/*

OTHER (balance adjustments) - 
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘8’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

INVTOLL
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘38’,’42’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

ESCTOLL
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘38’,’42’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

CLTOLL (collection toll)
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘38’,’42’) and COLL_COURT_FLAG = ‘COLL’ and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’) 
gives latest document  (Collection assignment is not associated to a document creation )

UTCTOLL
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘38’,’42’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID =’3’

COURTTOLL
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘38’,’42’) and COLL_COURT_FLAG = ‘CRT’ and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’) 
gives latest document  (Court assignment is not associated to a document creation )

COURTPLEATOLL
Join VB_ACTIVITY.KS_LEDGER_EVENT_TYPE_ID =’89’ and COLL_COURT_FLAG is null and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’) 

REGHOLDTOLL
Join ST_ACCT_FLAGS.ACCT_NUM = ST_document_info.acct_num and ST_ACCT_FLAG.FLAG_TYPE =’9’ and END_DATE is null, 
All the following tolls are on REG HOLD
JOIN ID of KS_LEDGER to LEDGER_ID of VB_ACTVITY for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID of PA_LANE_LANE_TXN and 
JOIN PLAZA_ID of PA_PLAZA to EXT_PLAZA_ID for PA_Lane_txn
  IF SUM (VB_ACTIVITY.AMT_CHARGED – VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS grouped by KS_LEDGER.ACCT_NUM
  FOR PA_PLAZA.FTE_PLAZA = 'Y',VB_ACTIVITY.COLL_COURT_FLAG is NULL, VB_ACTIVITY.CHILD_DOC_ID not null,VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and VB_ACTIVITY.BANKRUPTCY_flag is null
  THEN 'REG STOP'
  
ADMINFEE
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘48’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

ESCADMINFEE
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘48’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

CLADMINFEE
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘48’) and COLL_COURT_FLAG = ‘COLL’ and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’) 
gives latest document  (Collection assignment is not associated to a document creation )

UTCFEE
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘25’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID =’3’

CRTPLEAFEE
Join VB_ACTIVITY.KS_LEDGER_EVENT_TYPE_ID =’100’ and COLL_COURT_FLAG is null and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

COURTUTCFEE
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘25’) and COLL_COURT_FLAG = ‘CRT’ and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’) 
gives latest document  (Court assignment is not associated to a document creation )

EXPLANECHARGE (express lane charge)
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘145’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

ESCEXPPLANCECHARGE
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘145’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

MAILFEE (include a statement fee in case this is in the data)- We do not do this currently

NSF
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘17’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

ESCNSF
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘17’) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’)

CLNSF
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘17’) and COLL_COURT_FLAG = ‘COLL’ and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’) 
gives latest document  (Collection assignment is not associated to a document creation )


CLEXPLANECHARGE
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (‘145’) and COLL_COURT_FLAG = ‘COLL’ and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (‘1’,’2’) 
gives latest document  (Collection assignment is not associated to a document creation )


**Discussed bankruptcy, deceased, court – these will be tracked as statuses – 
use Bankruptcy flag in VB_Activity to BKTY in above queries to get this status, 
                                                                                                                                             
For ST_document_info. Acct_num joined PA_ACCT_DETAIL.DECEASED_DATE
CRT is provided above
*/

