DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;

DROP TABLE IF EXISTS gold.videoCategory;
CREATE TABLE gold.videoCategory (
    categoryId VARCHAR(5),
    categoryName VARCHAR(50)
);

DROP TABLE IF EXISTS gold.linkVideos;
CREATE TABLE gold.linkVideos (
    video_id VARCHAR(20),
    link_video VARCHAR(50)
);

DROP TABLE IF EXISTS gold.metricVideos;
CREATE TABLE gold.metricVideos (
    video_id VARCHAR(20),
    -- country_code,
    publishedAt TIMESTAMP,
    trending_date TIMESTAMP,
    channelId VARCHAR(27),
    categoryId VARCHAR(5),
    view_count INTEGER,
    likes INTEGER,
    dislikes INTEGER,
    comment_count INTEGER
);

DROP TABLE IF EXISTS gold.informationVideos;
CREATE TABLE gold.informationVideos (
    video_id VARCHAR(20),
    -- country_code,
    title TEXT,
    channelId VARCHAR(27),
    channelTitle TEXT,
    categoryId VARCHAR(5),
    tags TEXT,
    thumbnail_link TEXT,
    comments_disabled VARCHAR(5),
    ratings_disabled VARCHAR(5)
);