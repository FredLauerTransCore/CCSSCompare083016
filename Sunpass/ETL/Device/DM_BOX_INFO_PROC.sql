/********************************************************
*
* Name: DM_BOX_INFO_PROC
* Created by: DT, 4/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_BOX_INFO

We need to get more info:
1. how to get the STORE from PA_ACCT.ACCTTYPE_ACCT_TYPE_CODE
2. how to get BOX_CHECKOUT_TO and BOX_CHECKOUT_BY
3. How to get CREATED and CREATED_BY from PA_ACCT_TRANSP
4. How to get LAST_UPD, LAST_UPD_BY

*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_BOX_INFO_PROC IS
  CURSOR c1
  IS
    SELECT TRAY_TRAY_NUM ,
      TRAY_CASE_CASE_NUM,
      TRAY_CASE_LOT_LOT_NUM,
      COUNT(*) cnt,
	  min(INVTRANSP_TRANSP_ID) min_id,
	  max(INVTRANSP_TRANSP_ID) max_id
    FROM PA_INV_TRANSP 
    GROUP BY tRAY_TRAY_NUM ,
      TRAY_CASE_CASE_NUM ,
      TRAY_CASE_LOT_LOT_NUM;
  v_seq        NUMBER:=0;
  v_box_number VARCHAR2(30);
  v_BOX_CHECKOUT_TO VARCHAR2(30);
  v_BOX_CHECKOUT_BY VARCHAR2(30);
BEGIN
  FOR e1 IN c1
  LOOP
    v_seq:=v_seq+1;
       v_box_number:=e1.TRAY_TRAY_NUM||e1.TRAY_CASE_CASE_NUM||e1.TRAY_CASE_LOT_LOT_NUM;
	   
	    /* get PA_INV_TRANSP_ASSGN.EMP_EMP_CODE for BOX_CHECKOUT_TO */
     begin
        select EMP_EMP_CODE, EMP_EMP_CODE 
		into v_BOX_CHECKOUT_TO , v_BOX_CHECKOUT_BY
		from PA_INV_TRANSP_ASSGN where 
		 TRAY_TRAY_NUM=e1.TRAY_TRAY_NUM and
         TRAY_CASE_CASE_NUM=e1.TRAY_CASE_CASE_NUM and
         TRAY_CASE_LOT_LOT_NUM=e1.TRAY_CASE_LOT_LOT_NUM and
		rownum<=1;
  
        exception when others then null;
        v_BOX_CHECKOUT_TO:=null;
        v_BOX_CHECKOUT_BY:=null;
     end;

      INSERT
      INTO dm_box_info
        (
          BOX_NUMBER ,
          BOX_TYPE ,
          BOX_STATUS ,
          DEVICE_MODEL ,
          VEHICLE_CLASS ,
          STORE ,
          DEVICE_COUNT ,
          START_DEVICE_NUMBER ,
          END_DEVICE_NUMBER ,
          BOX_SHELF_RACK# ,
          BOX_CHECKOUT_TO ,
          BOX_CHECKOUT_BY ,
          DISPOSE_DATE ,
          CREATED ,
          CREATED_BY ,
          LAST_UPD ,
          LAST_UPD_BY ,
          SOURCE_SYSTEM
        )
      SELECT v_BOX_NUMBER BOX_NUMBER ,
        it.TRAY_CASE_CASE_NUM BOX_TYPE ,
        'INVENTORY' BOX_STATUS ,
        it.TRANSPTYPE_TRANSP_TYPE_CODE DEVICE_MODEL ,
        '02' VEHICLE_CLASS ,
        'CSC STORE' STORE ,
        e1.cnt DEVICE_COUNT ,
        e1.min_id START_DEVICE_NUMBER ,
        e1.max_id END_DEVICE_NUMBER ,
        NULL BOX_SHELF_RACK# ,
        v_BOX_CHECKOUT_TO , v_BOX_CHECKOUT_BY,
        NULL DISPOSE_DATE ,
        to_date('01/01/1900','MM/DD/YYYY') CREATED ,
        'SUNPASS_CSC_ID' CREATED_BY ,
        to_date('01/01/1900','MM/DD/YYYY') LAST_UPD ,
        'SUNPASS_CSC_ID' LAST_UPD_BY ,
        'SUNPASS' SOURCE_SYSTEM
      FROM PA_INV_TRANSP it
      WHERE TRAY_TRAY_NUM            =e1.TRAY_TRAY_NUM
      AND TRAY_CASE_CASE_NUM         =e1.TRAY_CASE_CASE_NUM
      AND TRAY_CASE_LOT_LOT_NUM      =e1.TRAY_CASE_LOT_LOT_NUM
      AND rownum                    <=1;
      commit;
 
  END LOOP;
  COMMIT;
END;
/