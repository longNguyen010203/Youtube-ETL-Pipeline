LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/DE_youtube_trending_data.csv' 
INTO TABLE DE_youtube_trending_data 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/JP_youtube_trending_data.csv' 
INTO TABLE JP_youtube_trending_data 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/KR_youtube_trending_data.csv'
INTO TABLE KR_youtube_trending_data 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/RU_youtube_trending_data.csv' 
INTO TABLE RU_youtube_trending_data 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/US_youtube_trending_data.csv' 
INTO TABLE US_youtube_trending_data 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;