CREATE TABLE AvgRatingTable (MovieId INT, Year INT, Name STRING, Rating DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'; 

LOAD DATA inpath '/user/avgrating' into table AvgRatingTable;

describe AvgRatingTable;

SELECT maxentry.yearrange, s.movieId, s.rating, s.year, s.name FROM AvgRatingTable s Join 
(SELECT yearrangetable.range AS yearrange, MAX(rating) AS maxrating FROM (SELECT movieid, CASE
	when Year < 1920 then '<1920'
	when Year between 1921 and 1930 then '1921 - 1930'
	when Year between 1931 and 1940 then '1931 - 1940'
	when Year between 1941 and 1950 then '1941 - 1950'
	when Year between 1951 and 1960 then '1951 - 1960'
  when Year between 1961 and 1970 then '1961 - 1970'
  when Year between 1971 and 1980 then '1971 - 1980'
  when Year between 1981 and 1990 then '1981 - 1990'
  when Year between 1991 and 2000 then '1991 - 2000'
  when Year between 2001 and 2010 then '2001 - 2010'
  else '2011-onwards'
  END AS range, rating, name FROM AvgRatingTable) yearrangetable
GROUP BY yearrangetable.range) maxentry ON s.rating = maxentry.maxrating ;

-- Quering the hive table created using 'bin/hive' command and storing the obtained result into CSV
-- For example:
-- $HIVE_HOME/bin/hive -f ~/Desktop/mysqlscript.sql > ~/Desktop/output.csv