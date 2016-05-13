/********************************************************
*
* Name: DM_AGENCY_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_AGENCY_INFO
* issue:
* 1. Need     DM_AGENCY_INFO_seq for AGENCY_ID, currently use rownum
* 2. Need to lengthen AGENCY_SHORT_NAME to more than 4 chars,
                      DEVICE_PREFIX to more than 3 chars,
                      FILE_PREFIX to more than 3 chars
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
    ,TITLE AGENCY_NAME 
    ,substr(AGENCY_NAME,1,4) AGENCY_SHORT_NAME 
    ,decode(IAG_HOME,'Y','IAG','FL') CONSORTIUM 
    ,'1.5' CURRENT_IAG_VERSION 
    ,substr(AGENCY_ID,1,3) DEVICE_PREFIX 
    ,substr(AGENCY_ID,1,3) FILE_PREFIX 
    ,decode(AGENCY_TYPE,'HA','Y','N') IS_HOME_AGENCY 
    ,AGENCY_ID PARENT_AGENCY_ID 
    ,0 STMT_DEFAULT_DURATION 
    ,0 STMT_DELIVERY /*NUMBER 22 Y*/
    ,AGENCY_NAME STMT_DESCRIPTION 
    ,0 STMT_FREQUENCY 
    ,AGENCY_ID AGENCY_CODE 
    ,'SUNPASS' SOURCE_SYSTEM 
FROM patron.ST_INTEROP_AGENCIES; 

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_AGENCY_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

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


