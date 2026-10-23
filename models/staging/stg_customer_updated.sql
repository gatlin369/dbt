{{
    config(
        materialized='view'
    )
}}

SELECT
    C_CUSTOMER_ID AS CUSTOMER_ID,
    C_FIRST_NAME AS FIRST_NAME,
    C_LAST_NAME AS LAST_NAME,
    C_BIRTH_COUNTRY AS COUNTRY
FROM {{ ref('stg_customer') }}
