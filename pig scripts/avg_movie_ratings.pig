Movie_Titles = LOAD '/user/Dataset/movie_titles.txt' using PigStorage(',') as (id:int, year:int, name: chararray);
Movie_Ratings = LOAD '/user/Dataset/movie_ratings.txt' using PigStorage(',') as (id:int, userid:int, rating:int, date: chararray);
Group_By_Id = group Movie_Ratings by id;
Avg_Rating = FOREACH Group_By_Id GENERATE group as id, AVG(Movie_Ratings.$2) as avgrating;
Join_By_Id = JOIN Movie_Titles by $0, Avg_Rating by id using 'replicated';
STORE Join_By_Id INTO '/user/output/pig/movies_avg_ratings';