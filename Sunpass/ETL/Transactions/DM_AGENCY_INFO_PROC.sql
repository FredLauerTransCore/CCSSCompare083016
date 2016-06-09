/********************************************************
*
* Name: DM_AGENCY_INFO_PROC
* Created by: DT, 4/21/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_AGENCY_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_AGENCY_INFO_PROC IS

TYPE DM_AGENCY_INFO_TYP IS TABLE OF DM_AGENCY_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_AGENCY_INFO_tab DM_AGENCY_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    rownum AGENCY_ID
    ,CASE AGENCY__ID 
      WHEN  101 THEN 'FL CCSS'
      WHEN  61  THEN 'FTE'
      WHEN  106 THEN 'Ft. Lauderdale Airport'
      WHEN  65  THEN 'LCF'
      WHEN  63  THEN 'MDX'
      WHEN  104 THEN 'Miami Airport'
      WHEN  30  THEN 'NCTA'
      WHEN  62  THEN 'OOCEA'
      WHEN  105 THEN 'Orlando Airport'
      WHEN  102 THEN 'Palm Beach Airport'
      WHEN  34  THEN 'SRTA'
      WHEN  64  THEN 'THEAS' 
      WHEN  103 THEN 'Tampa Airport'  
    END col_name --to be dertermined
    ,TITLE AGENCY_NAME
    ,AGENCY_NAME AGENCY_SHORT_NAME
    ,decode(IAG_HOME,'Y','IAG','FL') CONSORTIUM
    ,'1.51' CURRENT_IAG_VERSION
    ,AGENCY_ID DEVICE_PREFIX
    ,AGENCY_ID FILE_PREFIX
    ,decode(AGENCY_TYPE,'HA','Y','N') IS_HOME_AGENCY
    ,AGENCY_ID PARENT_AGENCY_ID
    ,0 STMT_DEFAULT_DURATION
    ,0 STMT_DELIVERY
    ,AGENCY_NAME STMT_DESCRIPTION
    ,0 STMT_FREQUENCY
    ,AGENCY_ID AGENCY_CODE
    ,'SUNPASS' SOURCE_SYSTEM
FROM PATRON.ST_INTEROP_AGENCIES;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_AGENCY_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_AGENCY_INFO_tab.first .. DM_AGENCY_INFO_tab.last
           INSERT INTO DM_AGENCY_INFO VALUES DM_AGENCY_INFO_tab(i);
                       
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


