{{ config(materialized="table") }}

SELECT
    video_id,
    link_video
FROM {{ source("youtube", "linkvideos") }}