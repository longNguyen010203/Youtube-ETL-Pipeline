DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;

CREATE TABLE gold.videoCategory (
    categoryId VARCHAR(5),
    categoryName VARCHAR(50)
);

CREATE TABLE gold.linkVideos (
    video_id VARCHAR(20),
    link_video VARCHAR(50)
);