Movie_Ratings = LOAD '/user/Dataset/movie_ratings.txt' using PigStorage(',') as (id:int, userid:int, rating:int, date: chararray);
Group_By_User = group Movie_Ratings by userid;
Movie_Count = FOREACH Group_By_User GENERATE group as userid, COUNT(Movie_Ratings.$0) as movie_Count;
Movie_Count_Desc = Order Movie_Count by movie_Count DESC;
STORE Movie_Count_Desc INTO '/user/output/pig/rating_count_per_cus';