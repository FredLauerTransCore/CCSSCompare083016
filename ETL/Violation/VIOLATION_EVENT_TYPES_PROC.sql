/********************************************************
*
* Name: VIOLATION_EVENT_TYPES_PROC
* Created by: DT, 4/8/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              VIOLATION_EVENT_TYPES
*
********************************************************/

CREATE OR REPLACE PROCEDURE VIOLATION_EVENT_TYPES_PROC IS

TYPE VIOLATION_EVENT_TYPESTyp IS TABLE OF VIOLATION_EVENT_TYPES%ROWTYPE 
     INDEX BY BINARY_INTEGER;
VIOLATION_EVENT_TYPES_tab VIOLATION_EVENT_TYPESTyp;

l_VIOLATION_EVENT_TYPE_ID dbms_sql.NUMBER_table;
l_STATE_MODEL_ID dbms_sql.NUMBER_table;
l_CODE dbms_sql.VARCHAR2_table;
l_DESCRIPTION dbms_sql.VARCHAR2_table;
l_LABEL dbms_sql.VARCHAR2_table;
p_count dbms_sql.number_table;
P_ARRAY_SIZE NUMBER:=10000;

/* Change the FTE_SOURCE_TABLE to the actual table              */

CURSOR C1 IS SELECT * FROM patron.VIOLATION_EVENT_TYPES@mysuntoll;

BEGIN

  OPEN C1;

  LOOP

  /*Bulk select */
  FETCH C1 BULK COLLECT INTO
  l_VIOLATION_EVENT_TYPE_ID
  ,l_STATE_MODEL_ID
  ,l_CODE
  ,l_DESCRIPTION
  ,l_LABEL
  LIMIT P_ARRAY_SIZE;

  /*Bulk insert */                           
  FORALL I IN 1 .. l_VIOLATION_EVENT_TYPE_ID.COUNT
    INSERT INTO VIOLATION_EVENT_TYPES
    ( VIOLATION_EVENT_TYPE_ID
    ,STATE_MODEL_ID
    ,CODE
    ,DESCRIPTION
    ,LABEL
    )
    values
    ( l_VIOLATION_EVENT_TYPE_ID(i)
    ,l_STATE_MODEL_ID(i)
    ,l_CODE(i)
    ,l_DESCRIPTION(i)
    ,l_LABEL(i)
    ) 
      ;
    EXIT WHEN C1%NOTFOUND;
  END LOOP;

  COMMIT;

  CLOSE C1;

  COMMIT;
END;
/
SHOW ERRORS

