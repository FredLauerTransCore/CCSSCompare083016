/********************************************************
*
* Name: DM_PLAZA_INFO_PROC
* Created by: JL, 6/13/2016
* Revision: 1.0
* Description: This is the mapping script for
*              DM_PLAZA_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE SEQUENCE DM_PLAZA_INFO_PLAZA_ID_SEQ
   INCREMENT BY 1
   START WITH 1
   NOMAXVALUE
   NOCYCLE
   CACHE 100;

CREATE OR REPLACE PROCEDURE DM_PLAZA_INFO_PROC IS

TYPE DM_PLAZA_INFO_TYP IS TABLE OF DM_PLAZA_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PLAZA_INFO_tab DM_PLAZA_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    PA.PLAZA_ID PLAZA_ID
    ,IAG_PLAZA_ID EXTERN_PLAZA_ID
    ,PLAZA_NAME NAME
    ,'0' ADDRESS_ID
    ,to_date('01/01/1900','MM/DD/YYYY') OPENED_DT
    ,'0' DMQ_PLAZA_MASK
    ,SYSDATE UPDATE_TS
    ,SIA.AGENCY_ID AGENCY_ID
    ,'0' DEFAULT_PLAN_ID
    ,'00:00:00' REVENUE_TIME
    ,'0' COURT_ID
    ,'0' PLAZA_GROUP
    ,'000.000.000.000' IP_ADDRESS
    ,'CCSS' NODENAME
    ,'CCSS' USERNAME
    ,'CCSS' PASSWORD
    ,'N/A' TS_DIR
    ,'N/A' TX_DIR
    ,'N/A' IMAGE_INCOMING_VOLUME
    ,'N/A' IMAGE_ARCHIVE_VOLUME
    ,DECODE(SUBSTR(PLAZA_ID,1,3),'004','C','B') PLAZA_TYPE_IND
    ,'N' END_PLAZA_IND
    ,'N' SECTION_CODE_IND
    ,'0' SECTION_ID
    ,PLAZA_SHORT_NAME PLAZA_ABBR
    ,'0' PLAZA_SEQ_NUMBER
    ,'0' MILEAGE
    ,'0' PLAZA_MASK
    ,'0' HWY_NO
    ,FTE_PLAZA IS_HOME_PLAZA
    ,'0' PORT_NO
    ,'N' CALCULATE_TOLL_AMOUNT
    ,'N' LPR_ENABLED
    ,NULL GRACE_PERIOD
    ,NULL GRACE_TIME
    ,CTY.COUNTY_CODE_DESC JURISDICTION
    ,'NA' IMAGE_RESOLUTION
    ,FACCODE_FACILITY_CODE FACILITY_ID
    ,'0' JURISDICTION_CODE
    ,'0' POSTING_TYPE
    ,PLAZA_ID HOST_PLAZA_ID
    ,'SUNPASS' SOURCE_SYSTEM
    ,UTURN_INTERVAL POACHING_LIMIT
FROM PA_PLAZA PA,
     PA_COUNTY CTY,
     ST_INTEROP_AGENCIES SIA
WHERE PA.COUNTY_COUNTY_CODE = CTY.COUNTY_CODE(+)
AND   PA.AUTHCODE_AUTHORITY_CODE = SIA.AUTHORITY_CODE(+);

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PLAZA_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    FOR i in 1 .. DM_PLAZA_INFO_tab.count loop
    
      /* get PLAZA_LANE_TYPE_DIRECTION.PLAZA_ID for PLAZA_ID */
      begin
        select td.PLAZA_ID into DM_PLAZA_INFO_tab(i).PLAZA_ID
        from PLAZA_LANE_TYPE_DIRECTION td 
        where td.PLAZA_ID=DM_PLAZA_INFO_tab(i).PLAZA_ID
              and rownum<=1;
        exception 
          when others then null;
          DM_PLAZA_INFO_tab(i).PLAZA_ID:=null;
      end;    
    
	    /* to default the values NOT NULL columns */
	 if DM_PLAZA_INFO_tab(i).HOST_PLAZA_ID is null then
          DM_PLAZA_INFO_tab(i).HOST_PLAZA_ID:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).PLAZA_ID is null then
          DM_PLAZA_INFO_tab(i).PLAZA_ID:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).EXTERN_PLAZA_ID is null then
          DM_PLAZA_INFO_tab(i).EXTERN_PLAZA_ID:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).NAME is null then
          DM_PLAZA_INFO_tab(i).NAME:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).ADDRESS_ID is null then
          DM_PLAZA_INFO_tab(i).ADDRESS_ID:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).OPENED_DT is null then
          DM_PLAZA_INFO_tab(i).OPENED_DT:=sysdate;
         end if;
	 if DM_PLAZA_INFO_tab(i).DMQ_PLAZA_MASK is null then
          DM_PLAZA_INFO_tab(i).DMQ_PLAZA_MASK:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).UPDATE_TS is null then
          DM_PLAZA_INFO_tab(i).UPDATE_TS:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).AGENCY_ID is null then
          DM_PLAZA_INFO_tab(i).AGENCY_ID:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).DEFAULT_PLAN_ID is null then
          DM_PLAZA_INFO_tab(i).DEFAULT_PLAN_ID:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).REVENUE_TIME is null then
          DM_PLAZA_INFO_tab(i).REVENUE_TIME:=sysdate;
         end if;
	 if DM_PLAZA_INFO_tab(i).COURT_ID is null then
          DM_PLAZA_INFO_tab(i).COURT_ID:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).PLAZA_GROUP is null then
          DM_PLAZA_INFO_tab(i).PLAZA_GROUP:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).IP_ADDRESS is null then
          DM_PLAZA_INFO_tab(i).IP_ADDRESS:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).NODENAME is null then
          DM_PLAZA_INFO_tab(i).NODENAME:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).USERNAME is null then
          DM_PLAZA_INFO_tab(i).USERNAME:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).PASSWORD is null then
          DM_PLAZA_INFO_tab(i).PASSWORD:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).PLAZA_TYPE_IND is null then
          DM_PLAZA_INFO_tab(i).PLAZA_TYPE_IND:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).END_PLAZA_IND is null then
          DM_PLAZA_INFO_tab(i).END_PLAZA_IND:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).SECTION_CODE_IND is null then
          DM_PLAZA_INFO_tab(i).SECTION_CODE_IND:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).SECTION_ID is null then
          DM_PLAZA_INFO_tab(i).SECTION_ID:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).PLAZA_ABBR is null then
          DM_PLAZA_INFO_tab(i).PLAZA_ABBR:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).MILEAGE is null then
          DM_PLAZA_INFO_tab(i).MILEAGE:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).JURISDICTION is null then
          DM_PLAZA_INFO_tab(i).JURISDICTION:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).FACILITY_ID is null then
          DM_PLAZA_INFO_tab(i).FACILITY_ID:='0';
         end if;
	 if DM_PLAZA_INFO_tab(i).JURISDICTION_CODE is null then
          DM_PLAZA_INFO_tab(i).JURISDICTION_CODE:='0';
         end if;
    end loop;

	
    /*ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_PLAZA_INFO_tab.first .. DM_PLAZA_INFO_tab.last
           INSERT INTO DM_PLAZA_INFO VALUES DM_PLAZA_INFO_tab(i);
                       
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


