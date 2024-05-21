DROP TABLE IF EXISTS youtube_trending_data;


DROP TABLE IF EXISTS CA_youtube_trending_data;
CREATE TABLE CA_youtube_trending_data (
    video_id VARCHAR(20),
    title TEXT,
    publishedAt VARCHAR(27),
    channelId VARCHAR(27),
    channelTitle TEXT,
    categoryId VARCHAR(5),
    trending_date VARCHAR(27),
    tags TEXT,
    view_count TEXT,
    likes TEXT,
    dislikes TEXT,
    comment_count TEXT,
    thumbnail_link TEXT,
    comments_disabled VARCHAR(6),  
    ratings_disabled VARCHAR(6)
);

DROP TABLE IF EXISTS DE_youtube_trending_data;
CREATE TABLE DE_youtube_trending_data (
    video_id VARCHAR(20),
    title TEXT,
    publishedAt VARCHAR(27),
    channelId VARCHAR(27),
    channelTitle TEXT,
    categoryId VARCHAR(5),
    trending_date VARCHAR(27),
    tags TEXT,
    view_count TEXT,
    likes TEXT,
    dislikes TEXT,
    comment_count TEXT,
    thumbnail_link TEXT,
    comments_disabled VARCHAR(6),  
    ratings_disabled VARCHAR(6)
);

DROP TABLE IF EXISTS IN_youtube_trending_data;
CREATE TABLE IN_youtube_trending_data (
    video_id VARCHAR(20),
    title TEXT,
    publishedAt VARCHAR(27),
    channelId VARCHAR(27),
    channelTitle TEXT,
    categoryId VARCHAR(5),
    trending_date VARCHAR(27),
    tags TEXT,
    view_count TEXT,
    likes TEXT,
    dislikes TEXT,
    comment_count TEXT,
    thumbnail_link TEXT,
    comments_disabled VARCHAR(6),  
    ratings_disabled VARCHAR(6)
);

DROP TABLE IF EXISTS JP_youtube_trending_data;
CREATE TABLE JP_youtube_trending_data (
    video_id VARCHAR(20),
    title TEXT,
    publishedAt VARCHAR(27),
    channelId VARCHAR(27),
    channelTitle TEXT,
    categoryId VARCHAR(5),
    trending_date VARCHAR(27),
    tags TEXT,
    view_count TEXT,
    likes TEXT,
    dislikes TEXT,
    comment_count TEXT,
    thumbnail_link TEXT,
    comments_disabled VARCHAR(6),  
    ratings_disabled VARCHAR(6)
);

DROP TABLE IF EXISTS RU_youtube_trending_data;
CREATE TABLE RU_youtube_trending_data (
    video_id VARCHAR(20),
    title TEXT,
    publishedAt VARCHAR(27),
    channelId VARCHAR(27),
    channelTitle TEXT,
    categoryId VARCHAR(5),
    trending_date VARCHAR(27),
    tags TEXT,
    view_count TEXT,
    likes TEXT,
    dislikes TEXT,
    comment_count TEXT,
    thumbnail_link TEXT,
    comments_disabled VARCHAR(6),  
    ratings_disabled VARCHAR(6)
);