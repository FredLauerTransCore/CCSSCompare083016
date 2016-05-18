select * from DM_VEHICLE_INFO where rownum<101;

SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
--    ,DECODE(VEH_LIC_NUM, 'O', '0', VEH_LIC_NUM) PLATE_NUMBER
--    ,STATE_STATE_CODE_ABBR PLATE_STATE
--    ,'USA' PLATE_COUNTRY   -- Derive from State	PLATE_COUNTRY	USA (Default), CAN, MEX
--    ,VEH_LIC_TYPE PLATE_TYPE    -- Prefix and Suffix for the Country and State.  
--                                -- Discussion on how FL prefix & suffixes will be handled is an outstanding action item.
--                                -- Sunny will provide sample FL plates for analysis that will have associated type info.
--    ,'REGULAR' VEHICLE_TYPE    --	"REGULAR (Default), AUTO / SUV ?
--    ,SUBSCRIPTION_START_DATE EFFECTIVE_START_DATE
--    ,SUBSCRIPTION_END_DATE EFFECTIVE_END_DATE
--    ,nvl(VEH_MODEL_YR, '9999') YEAR
--    ,nvl(VEH_MAKE, 'OTHER') MAKE
--    ,nvl(VEH_MODEL, 'OTHER') MODEL
--    ,VEH_COLOR COLOUR
--    ,REG_STOP_SENT_ON REG_STOP_START_DATE  -- ST_REG_STOP
--    ,REG_STOP_REMOVAL_SENT_ON REG_STOP_END_DATE -- ST_REG_STOP
--    ,NULL METAL_OXIDE_WIND_SHIELD
--    ,NULL DMV_RETURN_DATE  -- suntoll    --MAX(RESPONSE_DATE)Event lookup ROV
--    ,to_date('01-01-1900', 'MM-DD-YYYY') DMV_RETURN_DATE
--    ,NULL VEHICLE_CLASS
--    ,NULL AXLE_COUNT
--    ,NULL IS_DUAL_TIRE
--    ,RENTAL_FLAG IS_RENTAL
--    ,NULL RENTAL_COMPANY
--    ,NULL RENTAL_COMPANY_PHONE
--    ,CREATION_DATE CREATED
--    ,EMP_EMP_CODE CREATED_BY
--    ,LAST_MODIFIED_DATE LAST_UPD
--    ,NULL LAST_UPD_BY   --	DEFAULT	SUNPASS_CSC_ID		LAST_UPD_BY	SUNPASS_CSC_ID (Default)
--    ,VEHICLE_SEQ VEHICLE_SEQUENCE
--    ,SUNTOLL SOURCE_SYSTEM
--    ,NULL PLATE_ISSUE_DATE    -- DERIVE
--    ,NULL PLATE_EXPIRY_DATE   -- DERIVE
--    ,NULL PLATE_RENEWAL_DATE  -- DERIVE
--    ,NULL ALT_VEHICLE_ID      -- DERIVE
FROM PATRON.PA_ACCT_VEHICLE
where rownum<11; 

select ACCT_ACCT_NUM ACCOUNT_NUMBER FROM PATRON.PA_ACCT_VEHICLE
where rownum<11; 

                                -- Discussion on how FL prefix & suffixes will be handled is an outstanding action item.
                                -- Sunny will provide sample FL plates for analysis that will have associated type info.
--    ,'REGULAR' VEHICLE_TYPE    --	 REGULAR (Default), AUTO / SUV 
SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,DECODE(VEH_LIC_NUM, 'O', '0', VEH_LIC_NUM) PLATE_NUMBER
    ,STATE_STATE_CODE_ABBR PLATE_STATE
    ,'USA' PLATE_COUNTRY   -- Derive from State	PLATE_COUNTRY	USA (Default), CAN, MEX
    ,VEH_LIC_TYPE PLATE_TYPE    -- Prefix and Suffix for the Country and State.  
--    ,SUBSCRIPTION_START_DATE EFFECTIVE_START_DATE
--    ,SUBSCRIPTION_END_DATE EFFECTIVE_END_DATE
    ,nvl(VEH_MODEL_YR, '9999') YEAR
    ,nvl(VEH_MAKE, 'OTHER') MAKE
    ,nvl(VEH_MODEL, 'OTHER') MODEL
    ,VEH_COLOR COLOUR
--    ,REG_STOP_SENT_ON REG_STOP_START_DATE  -- ST_REG_STOP
--    ,REG_STOP_REMOVAL_SENT_ON REG_STOP_END_DATE -- ST_REG_STOP
    ,NULL METAL_OXIDE_WIND_SHIELD
    ,NULL DMV_RETURN_DATE  -- suntoll    --MAX(RESPONSE_DATE)Event lookup ROV
    ,to_date('01-01-1900', 'MM-DD-YYYY') DMV_RETURN_DATE
    ,NULL VEHICLE_CLASS
    ,NULL AXLE_COUNT
    ,NULL IS_DUAL_TIRE
    ,RENTAL_FLAG IS_RENTAL
    ,NULL RENTAL_COMPANY
    ,NULL RENTAL_COMPANY_PHONE
    ,CREATION_DATE CREATED
    ,EMP_EMP_CODE CREATED_BY
    ,LAST_MODIFIED_DATE LAST_UPD
    ,NULL LAST_UPD_BY   --	DEFAULT	SUNPASS_CSC_ID		LAST_UPD_BY	SUNPASS_CSC_ID (Default)
 --   ,VEHICLE_SEQ VEHICLE_SEQUENCE
    ,'SUNTOLL' SOURCE_SYSTEM
    ,NULL PLATE_ISSUE_DATE    -- DERIVE
    ,NULL PLATE_EXPIRY_DATE   -- DERIVE
    ,NULL PLATE_RENEWAL_DATE  -- DERIVE
    ,NULL ALT_VEHICLE_ID      -- DERIVE
FROM PATRON.PA_ACCT_VEHICLE
where rownum<11; 

SELECT * from all_tables
where rownum<11
and owner = 'PATRON'
and table_name like '%STATE%'
;
select * from PATRON.PA_STATE_CODE
--PA_STATEMENT_OPT_CODE