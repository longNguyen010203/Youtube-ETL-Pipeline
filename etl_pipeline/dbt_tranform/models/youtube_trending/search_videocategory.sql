{{ config(materialized="table") }}

SELECT 
    categoryid,
    categoryname
FROM {{ source("youtube", "videocategory") }}
