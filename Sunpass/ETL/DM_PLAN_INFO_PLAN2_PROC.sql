/********************************************************
*
* Name: DM_PLAN_INFO_PLAN2_PROC
* Created by: DT, 4/20/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PLAN_INFO_PLAN2
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PLAN_INFO_PLAN2_PROC IS

TYPE DM_PLAN_INFO_PLAN2_TYP IS TABLE OF DM_PLAN_INFO_PLAN2%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PLAN_INFO_PLAN2_tab DM_PLAN_INFO_PLAN2_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,'BAYWAY COMMUTER' PLAN_NAME
    ,INVTRANSP_TRANSP_TRANSP_ID DEVICE_NUMBER
    ,BAYWAY_PASS2_DATE START_DATE
    ,case
     when to_char(BAYWAY_PASS2_DATE,'MMDD') >'1001' 
        then add_months(to_date('09/30/'||to_char(BAYWAY_PASS2_DATE,'YYYY'),'MM/DD/YYYY'),12)
     when to_char(BAYWAY_PASS2_DATE,'MMDD') <='1001'
        then to_date('09/30/'||to_char(BAYWAY_PASS2_DATE,'YYYY'),'MM/DD/YYYY')
     end  as END_DATE
    ,BAYWAY_PASS2_PAID PLAN_STATUS
    ,'N' AUTO_RENEW
    ,BAYWAY_PASS2_PAID CREATED
    ,NULL CREATED_BY
    ,BAYWAY_PASS2_PAID LAST_UPD
    ,NULL LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM PATRON.PA_ACCT_TRANSP;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PLAN_INFO_PLAN2_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_PLAN_INFO_PLAN2_tab.first .. DM_PLAN_INFO_PLAN2_tab.last
           INSERT INTO DM_PLAN_INFO_PLAN2 VALUES DM_PLAN_INFO_PLAN2_tab(i);
                       
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


