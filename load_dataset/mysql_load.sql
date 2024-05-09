LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/DE_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/JP_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/KR_youtube_trending_data.csv'
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/RU_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/US_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/BR_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/CA_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/FR_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/GB_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/IN_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/MX_youtube_trending_data.csv' 
INTO TABLE youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;