/********************************************************
*
* Name: DM_PA_PAYMENT_SOURCE_CODE_PROC
* Created by: DT, 4/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PA_PAYMENT_SOURCE_CODE
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PA_PAYMENT_SOURCE_CODE_PROC IS

TYPE DM_PA_PAYMENT_SOURCE_CODE_TYP IS TABLE OF DM_PA_PAYMENT_SOURCE_CODE%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PA_PAYMENT_SOURCE_CODE_tab DM_PA_PAYMENT_SOURCE_CODE_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    PAYMENT_SOURCE_CODE PAYMENT_SOURCE_CODE
    ,PAYMENT_SOURCE_DESC PAYMENT_SOURCE_DESC
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_PAYMENT_SOURCE_CODE;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PA_PAYMENT_SOURCE_CODE_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */
	
	    /* to default the values NOT NULL columns */
    FOR i in 1 .. DM_PA_PAYMENT_SOURCE_CO_tab.count loop
	 if DM_PA_PAYMENT_SOURCE_CO_tab(i).PAYMENT_SOURCE_CODE is null then
          DM_PA_PAYMENT_SOURCE_CO_tab(i).PAYMENT_SOURCE_CODE:='0';
         end if;
	 if DM_PA_PAYMENT_SOURCE_CO_tab(i).PAYMENT_SOURCE_DESC is null then
          DM_PA_PAYMENT_SOURCE_CO_tab(i).PAYMENT_SOURCE_DESC:='0';
         end if;
    end loop;


    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_PA_PAYMENT_SOURCE_CODE_tab.first .. DM_PA_PAYMENT_SOURCE_CODE_tab.last
           INSERT INTO DM_PA_PAYMENT_SOURCE_CODE VALUES DM_PA_PAYMENT_SOURCE_CODE_tab(i);
                       
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


