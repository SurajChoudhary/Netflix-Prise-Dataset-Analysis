-- Running queries using hive shell to create hive tables
CREATE TABLE CustomerPopularityAnalysis (CusId INT, CusRatingCount INT, AvgRating DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- loading data into the table from HDFS
LOAD DATA inpath '/user/CusAnalysisTable' into table CustomerAnalysis;

-- Quering the hive table created in previous step using 'bin/hive' command and storing the obtained result into CSV
-- example:
-- $HIVE_HOME/bin/hive -e 'Select * from CustomerPopularityAnalysis' > ~/Desktop/output.csv
-- The queries are constructed to obtaining data, which can help to classify the customers into different 
-- quadrants for Quadrant Analysis.

SELECT CusRatingCount, AvgRating as count FROM CustomerAnalysis WHERE  CusRatingCount > 1130 and AvgRating > 3.5;

SELECT CusRatingCount, AvgRating as count FROM CustomerAnalysis WHERE  CusRatingCount > 1130 and AvgRating <= 3.5;

SELECT CusRatingCount, AvgRating as count FROM CustomerAnalysis WHERE  CusRatingCount <= 1130 and AvgRating > 3.5;

SELECT CusRatingCount, AvgRating as count FROM CustomerAnalysis WHERE  CusRatingCount <= 1130 and AvgRating <= 3.5;