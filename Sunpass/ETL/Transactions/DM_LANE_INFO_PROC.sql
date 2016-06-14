/********************************************************
*
* Name: DM_LANE_INFO_PROC
* Created by: DT, 4/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_LANE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_LANE_INFO_PROC IS

TYPE DM_LANE_INFO_TYP IS TABLE OF DM_LANE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_LANE_INFO_tab DM_LANE_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL LANE_ID
    ,LANE_ID EXTERN_LANE_ID
    ,'Y' AVI
    ,LANE_TYPE OPERATIONAL_MODE
    ,'0' STATUS
    ,PLAZA_ID PLAZA_ID
    ,'0' LANE_IDX
    ,ASOF UPDATE_TS
    ,DIRECTION DIRECTION
    ,'1' LANE_TYPE
    ,'0' LANE_MASK
    ,'00.00.00.00' LANE_IP
    ,'0' PORT_NO
    ,'CCSS' HOST_QUEUE_NAME
    ,'N' CALCULATE_TOLL_AMOUNT
    ,NULL IMAGE_RESOLUTION
    ,NULL LPR_ENABLED
    ,PA_PLAZA.NULL ROADWAY
    ,PA_FACILITY_CODE.NULL STMT_DESC
    ,PA_FACILITY_CODE.NULL LOCATION
    ,'0' COURT_ID
    ,'fromST' JURISDICTION
    ,PA_PLAZA.NULL LANE_PLAZA_ABBR
    ,PA_PLAZA.NULL AGENCY_ID
    ,'SUNPASS' SOURCE_SYSTEM
FROM SUNPASS.PLAZA_LANE_TYPE_DIRECTION;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_LANE_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in 1 .. DM_LANE_INFO_tab.count loop

    /* get PA_PLAZA.FACCODE_FACILITY_CODE for ROADWAY */
    begin
      select FACCODE_FACILITY_CODE,
             PLAZA_SHORT_NAME,AGENCY_ID into DM_LANE_INFO_tab(i).ROADWAY,
             DM_LANE_INFO_tab(i).LANE_PLAZA_ABBR,
             DM_LANE_INFO_tab(i).AGENCY_ID
      from PA_PLAZA 
      where PLAZA_ID=DM_LANE_INFO_tab(i).PLAZA_ID
            and rownum<=1;
      exception 
        when others then null;
        DM_LANE_INFO_tab(i).ROADWAY:=null;
    end;


    /* get PA_FACILITY_CODE.FACILITY_DESC for STMT_DESC */
    begin
      select FACILITY_DESC,FACILITY_DESC into DM_LANE_INFO_tab(i).STMT_DESC,
             DM_LANE_INFO_tab(i).LOCATION from PA_FACILITY_CODE 
      where FACILITY_CODE=DM_LANE_INFO_tab(i).ROADWAY
            and rownum<=1;
      exception 
        when others then null;
        DM_LANE_INFO_tab(i).STMT_DESC:=null;
    end;

    
    end loop;



    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_LANE_INFO_tab.first .. DM_LANE_INFO_tab.last
           INSERT INTO DM_LANE_INFO VALUES DM_LANE_INFO_tab(i);
                       
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


