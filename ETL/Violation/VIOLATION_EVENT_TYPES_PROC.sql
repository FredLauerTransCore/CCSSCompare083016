/********************************************************
*
* Name: VIOLATION_EVENT_TYPES_PROC
* Created by: DT, 4/11/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              VIOLATION_EVENT_TYPES
*
********************************************************/

CREATE OR REPLACE PROCEDURE VIOLATION_EVENT_TYPES_PROC IS
l_VIOLATION_EVENT_TYPE_ID dbms_sql.NUMBER_table;
l_STATE_MODEL_ID dbms_sql.NUMBER_table;
l_CODE dbms_sql.VARCHAR2_table;
l_DESCRIPTION dbms_sql.VARCHAR2_table;
l_LABEL dbms_sql.VARCHAR2_table;

p_count dbms_sql.number_table;
P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1 IS SELECT * FROM FTE_SOURCE_TABLE; /* replace FTE_SOURCE_TABLE to actual table*/

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

  /*Bulk transform, l_VIOLATION_EVENT_TYPE_ID.COUNT is the upper limit of the loop */


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

    /*Summary, load status, process status update */

    COMMIT;

  END LOOP;


  CLOSE C1;

  /*Summary, load status, process status update */

  COMMIT;

END;
/
SHOW ERRORS

