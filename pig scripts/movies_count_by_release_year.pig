Movie_Titles = LOAD '/user/Dataset/movie_titles.txt' using PigStorage(',') as (id:int, year:int, name: chararray);
Group_By_Year = group Movie_Titles by year;
Count_Year = FOREACH Group_By_Year GENERATE group as year, COUNT(Movie_Titles.$0) as count; 
STORE Count_Year into '/user/output/pig/movies_count_by_release_year';