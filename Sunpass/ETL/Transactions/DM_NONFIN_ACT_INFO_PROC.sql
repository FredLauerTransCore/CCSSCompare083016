/********************************************************
*
* Name: DM_NONFIN_ACT_INFO_PROC
* Created by: DT, 5/8/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_NONFIN_ACT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_NONFIN_ACT_INFO_PROC IS

TYPE DM_NONFIN_ACT_INFO_TYP IS TABLE OF DM_NONFIN_ACT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_NONFIN_ACT_INFO_tab DM_NONFIN_ACT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,NONFIN_ACT_SEQ.nextval ACTIVITY_NUMBER
    ,NOTEPROB_NOTES_PROB_CODE CATEGORY
    ,NOTEPROB_NOTES_PROB_CODE SUB_CATEGORY
    ,'Non Financial' ACTIVITY_TYPE
    ,substr(NOTE,1,248) DESCRIPTION
    ,NOTE_DATE_TIME CREATED
    ,EMP_EMP_CODE CREATED_BYc 
    ,NOTE_DATE_TIME LAST_UPD
    ,EMP_EMP_CODE LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_ACCT_NOTE;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_NONFIN_ACT_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_NONFIN_ACT_INFO_tab.count loop
	 if DM_NONFIN_ACT_INFO_tab(i).ACCOUNT_NUMBER is null then
          DM_NONFIN_ACT_INFO_tab(i).ACCOUNT_NUMBER:='0';
         end if;
	 if DM_NONFIN_ACT_INFO_tab(i).ACTIVITY_NUMBER is null then
          DM_NONFIN_ACT_INFO_tab(i).ACTIVITY_NUMBER:='0';
         end if;
	 if DM_NONFIN_ACT_INFO_tab(i).CATEGORY is null then
          DM_NONFIN_ACT_INFO_tab(i).CATEGORY:='0';
         end if;
	 if DM_NONFIN_ACT_INFO_tab(i).SUB_CATEGORY is null then
          DM_NONFIN_ACT_INFO_tab(i).SUB_CATEGORY:='0';
         end if;
	 if DM_NONFIN_ACT_INFO_tab(i).DESCRIPTION is null then
          DM_NONFIN_ACT_INFO_tab(i).DESCRIPTION:='0';
         end if;
	 if DM_NONFIN_ACT_INFO_tab(i).CREATED is null then
          DM_NONFIN_ACT_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_NONFIN_ACT_INFO_tab(i).LAST_UPD is null then
          DM_NONFIN_ACT_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
    end loop;
	
    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_NONFIN_ACT_INFO_tab.first .. DM_NONFIN_ACT_INFO_tab.last
           INSERT INTO DM_NONFIN_ACT_INFO VALUES DM_NONFIN_ACT_INFO_tab(i);
                       
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


