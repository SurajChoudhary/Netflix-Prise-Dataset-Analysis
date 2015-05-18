Movie_Titles = LOAD '/user/Dataset/movie_titles.txt' using PigStorage(',') as (id:int, year:int, name: chararray);
Movie_Ratings = LOAD '/user/Dataset/movie_ratings.txt' using PigStorage(',') as (id:int, userid:int, rating:int, date: chararray);
Group_By_Id = group Movie_Ratings by id;
Avg_Rating = FOREACH Group_By_Id GENERATE group as id, AVG(Movie_Ratings.$2) as avgrating;
Join_By_Id = JOIN Movie_Titles by $0, Avg_Rating by id using 'replicated';
Movie_Name_Year_Rating = FOREACH Join_By_Id GENERATE name, year, avgrating;
Group_By_Year = GROUP Movie_Name_Year_Rating BY year;
Movie_Of_Year = FOREACH Group_By_Year GENERATE group as Group_By_Year, MAX(Movie_Name_Year_Rating.avgrating), Movie_Name_Year_Rating.name as maxrating;
STORE Movie_Of_Year INTO '/user/output/pig/popular_movie_by_release_year';