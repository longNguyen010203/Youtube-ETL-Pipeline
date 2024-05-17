DROP SCHEMA IF EXISTS youtube CASCADE;
CREATE SCHEMA youtube;

CREATE TABLE gold.videoCategory (
    categoryId VARCHAR(5),
    categoryName VARCHAR(50)
);

CREATE TABLE gold.linkVideos (
    video_id VARCHAR(20),
    link_video VARCHAR(50)
);