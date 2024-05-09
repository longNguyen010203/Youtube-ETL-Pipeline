DROP TABLE IF EXISTS youtube_trending_data;
CREATE TABLE youtube_trending_data (
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