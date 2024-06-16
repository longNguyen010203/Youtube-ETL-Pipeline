
{{ config(materialized="table") }}

SELECT 
    categoryid,
    categoryname
FROM {{ source("gold", "videocategory") }}