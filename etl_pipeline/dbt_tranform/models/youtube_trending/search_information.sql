{{ config(materialized="table") }}

select distinct 
    i.video_id
    , i.title
    , i.channeltitle 
    , v.categoryname
    , m.view
    , m.like as likes
    , m.dislike
    , m.publishedat
    , l.link_video
    , i.tags
    , i.thumbnail_link
from {{ source('youtube', 'informationvideos') }} i 
    inner join {{ source('youtube', 'linkvideos') }} l on i.video_id = l.video_id 
    inner join {{ source('youtube', 'videocategory') }} v on i.categoryid = v.categoryid 
    inner join (
        SELECT 
            video_id
            , MAX(view_count) AS view
            , MAX(likes) as like
            , MAX(dislikes) as dislike
            , MAX(publishedat) as publishedat
        FROM {{ source('youtube', 'metricvideos') }}
        GROUP BY video_id
    ) AS m on i.video_id = m.video_id

    