LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/DE_youtube_trending_data.csv' 
INTO TABLE DE_youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/JP_youtube_trending_data.csv' 
INTO TABLE JP_youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/RU_youtube_trending_data.csv' 
INTO TABLE RU_youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/CA_youtube_trending_data.csv' 
INTO TABLE CA_youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/youTube_trending_video/IN_youtube_trending_data.csv' 
INTO TABLE IN_youtube_trending_data 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;