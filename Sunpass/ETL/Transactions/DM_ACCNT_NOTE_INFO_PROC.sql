/********************************************************
*
* Name: DM_ACCNT_NOTE_INFO_PROC
* Created by: DT, 4/21/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCNT_NOTE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_ACCNT_NOTE_INFO_PROC IS

TYPE DM_ACCNT_NOTE_INFO_TYP IS TABLE OF DM_ACCNT_NOTE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCNT_NOTE_INFO_tab DM_ACCNT_NOTE_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,NOTEPROB_NOTES_PROB_CODE NOTE_TYPE
    ,'SUNPASS' NOTE_SUB_TYPE
    ,NOTE NOTE
    ,NOTE_DATE_TIME NOTE_CREATED_DT
    ,NOTE_DATE_TIME CREATED
    ,EMP_EMP_CODE CREATED_BY
    ,NOTE_DATE_TIME LAST_UPD
    ,EMP_EMP_CODE LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_ACCT_NOTE
WHERE EMP_EMP_CODE NOT LIKE '99__'
AND   EMP_EMP_CODE NOT LIKE '0100'; -- Pull non-system users only

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCNT_NOTE_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */


    FOR i in 1 .. DM_ACCNT_NOTE_INFO_tab.count loop

    /* get NOTES_DESC.pa_note_prob_code for NOTE_TYPE */
    begin
      select substr(NOTES_DESC,1,30) into DM_ACCNT_NOTE_INFO_tab(i).NOTE_TYPE from pa_note_prob_code 
      where NOTES_PROB_CODE=DM_ACCNT_NOTE_INFO_tab(i).NOTE_TYPE
            and rownum<=1;
      exception 
        when others then null;
        DM_ACCNT_NOTE_INFO_tab(i).NOTE_TYPE:='UNDEFINED';
    end;



	 if DM_ACCNT_NOTE_INFO_tab(i).ACCOUNT_NUMBER is null then
          DM_ACCNT_NOTE_INFO_tab(i).ACCOUNT_NUMBER:='0';
         end if;
	 if DM_ACCNT_NOTE_INFO_tab(i).NOTE_TYPE is null then
          DM_ACCNT_NOTE_INFO_tab(i).NOTE_TYPE:='UNDEFINED';
         end if;
	 if DM_ACCNT_NOTE_INFO_tab(i).NOTE_SUB_TYPE is null then
          DM_ACCNT_NOTE_INFO_tab(i).NOTE_SUB_TYPE:='0';
         end if;
	 if DM_ACCNT_NOTE_INFO_tab(i).NOTE is null then
          DM_ACCNT_NOTE_INFO_tab(i).NOTE:='0';
         end if;
	 if DM_ACCNT_NOTE_INFO_tab(i).NOTE_CREATED_DT is null then
          DM_ACCNT_NOTE_INFO_tab(i).NOTE_CREATED_DT:=sysdate;
         end if;
	 if DM_ACCNT_NOTE_INFO_tab(i).CREATED is null then
          DM_ACCNT_NOTE_INFO_tab(i).CREATED:=sysdate;
         end if;
	 if DM_ACCNT_NOTE_INFO_tab(i).LAST_UPD is null then
          DM_ACCNT_NOTE_INFO_tab(i).LAST_UPD:=sysdate;
         end if;
    end loop;

	
    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_ACCNT_NOTE_INFO_tab.first .. DM_ACCNT_NOTE_INFO_tab.last
           INSERT INTO DM_ACCNT_NOTE_INFO VALUES DM_ACCNT_NOTE_INFO_tab(i);
                       
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


