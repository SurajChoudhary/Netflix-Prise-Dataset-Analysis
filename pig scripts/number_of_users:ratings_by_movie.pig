Movie_Ratings = LOAD '/user/Dataset/movie_ratings.txt' using PigStorage(',') as (id:int, userid:int, rating:int, date: chararray);
Group_By_Movie = group Movie_Ratings by id;
Count_Rating = FOREACH Group_By_Movie GENERATE group as id, COUNT(Movie_Ratings.userid) as ratingcount;
STORE Count_Rating INTO '/user/output/pig/number_of_usersORratings_by_movie';