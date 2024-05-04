DROP TABLE IF EXISTS DE_youtube_trending_data;
CREATE TABLE DE_youtube_trending_data (
    video_id VARCHAR(11),
    title TEXT,
    publishedAt DATETIME,
    channelId VARCHAR(24),
    channelTitle TEXT,
    categoryId INTEGER,
    trending_date DATETIME,
    tags TEXT,
    view_count INTEGER,
    likes INTEGER,
    dislikes INTEGER,
    comment_count INTEGER,
    thumbnail_link TEXT,
    comments_disabled BOOL,  
    ratings_disabled BOOL, 
    description TEXT,
    PRIMARY KEY(video_id)
);

DROP TABLE IF EXISTS JP_youtube_trending_data;
CREATE TABLE JP_youtube_trending_data (
    video_id VARCHAR(11),
    title TEXT,
    publishedAt DATETIME,
    channelId VARCHAR(24),
    channelTitle TEXT,
    categoryId INTEGER,
    trending_date DATETIME,
    tags TEXT,
    view_count INTEGER,
    likes INTEGER,
    dislikes INTEGER,
    comment_count INTEGER,
    thumbnail_link TEXT,
    comments_disabled BOOL,  
    ratings_disabled BOOL, 
    description TEXT,
    PRIMARY KEY(video_id)
);

DROP TABLE IF EXISTS KR_youtube_trending_data;
CREATE TABLE KR_youtube_trending_data (
    video_id VARCHAR(11),
    title TEXT,
    publishedAt DATETIME,
    channelId VARCHAR(24),
    channelTitle TEXT,
    categoryId INTEGER,
    trending_date DATETIME,
    tags TEXT,
    view_count INTEGER,
    likes INTEGER,
    dislikes INTEGER,
    comment_count INTEGER,
    thumbnail_link TEXT,
    comments_disabled BOOL,  
    ratings_disabled BOOL, 
    description TEXT,
    PRIMARY KEY(video_id)
);

DROP TABLE IF EXISTS RU_youtube_trending_data;
CREATE TABLE RU_youtube_trending_data (
    video_id VARCHAR(11),
    title TEXT,
    publishedAt DATETIME,
    channelId VARCHAR(24),
    channelTitle TEXT,
    categoryId INTEGER,
    trending_date DATETIME,
    tags TEXT,
    view_count INTEGER,
    likes INTEGER,
    dislikes INTEGER,
    comment_count INTEGER,
    thumbnail_link TEXT,
    comments_disabled BOOL,  
    ratings_disabled BOOL, 
    description TEXT,
    PRIMARY KEY(video_id)
);

DROP TABLE IF EXISTS US_youtube_trending_data;
CREATE TABLE US_youtube_trending_data (
    video_id VARCHAR(11),
    title TEXT,
    publishedAt DATETIME,
    channelId VARCHAR(24),
    channelTitle TEXT,
    categoryId INTEGER,
    trending_date DATETIME,
    tags TEXT,
    view_count INTEGER,
    likes INTEGER,
    dislikes INTEGER,
    comment_count INTEGER,
    thumbnail_link TEXT,
    comments_disabled BOOL,  
    ratings_disabled BOOL, 
    description TEXT,
    PRIMARY KEY(video_id)
);