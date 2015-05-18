-- Running queries using hive shell to create hive tables

CREATE TABLE MoviePopularityAnalysis (MovieId INT, Year INT, MovieRatingCount INT, AvgRating DOUBLE, Name STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'; 

-- loading data into the table from HDFS
LOAD DATA inpath '/user/MoviePopularityAnalysisTable' into table MoviePopularityAnalysis;

-- Quering the hive table created in previous step using 'bin/hive' command and storing the obtained result into CSV
-- example:
-- $HIVE_HOME/bin/hive -e 'Select * from MoviePopularityAnalysis' > ~/Desktop/output.csv
-- The queries are constructed to obtaining data, which can help to classify the movies into different 
-- quadrants for Quadrant Analysis.

SELECT MovieRatingCount, AvgRating as count FROM MoviePopularityAnalysis WHERE  MovieRatingCount > 6000 and AvgRating > 3.5;

SELECT MovieRatingCount, AvgRating as count FROM MoviePopularityAnalysis WHERE  MovieRatingCount > 6000 and AvgRating <= 3.5;

SELECT MovieRatingCount, AvgRating as count FROM MoviePopularityAnalysis WHERE  MovieRatingCount <= 6000 and AvgRating > 3.5;

SELECT MovieRatingCount, AvgRating as count FROM MoviePopularityAnalysis WHERE  MovieRatingCount <= 6000 and AvgRating <= 3.5;