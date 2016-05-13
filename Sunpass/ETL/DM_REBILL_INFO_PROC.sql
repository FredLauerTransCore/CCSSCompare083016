/********************************************************
*
* Name: DM_REBILL_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_REBILL_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_REBILL_INFO_PROC IS

TYPE DM_REBILL_INFO_TYP IS TABLE OF DM_REBILL_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_REBILL_INFO_tab DM_REBILL_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL ACCOUNT_NUMBER
    ,NULL PAY_TYPE
    ,NULL CREDIT_CARD_NUMBER_MASK
    ,NULL CREDIT_CARD_NUMBER
    ,NULL CC_EXP_MONTH
    ,NULL CC_EXP_YEAR
    ,NULL LAST_4_CC_NUMBER
    ,NULL ACH_BANK_ACCOUNT_NUMBER
    ,NULL ACH_BANK_ACCOUNT_NUMBER_MASK
    ,NULL ACH_BANK_ROUTING_NUMBER
    ,NULL LAST_4_ACH_NUMBER
    ,NULL SEQUENCE_NUMBER
    ,NULL CREATED
    ,NULL CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,NULL LAST_USED_DATE
    ,NULL SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_REBILL_INFO_tab
    LIMIT P_ARRAY_SIZE;
    EXIT WHEN C1%NOTFOUND;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_REBILL_INFO_tab.first .. DM_REBILL_INFO_tab.last
           INSERT INTO DM_REBILL_INFO VALUES DM_REBILL_INFO_tab(i);
                       

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


