Movie_Ratings = LOAD '/user/Dataset/movie_ratings.txt' using PigStorage(',') as (id:int, userid:int, rating:int, date: chararray);
Group_By_User = group Movie_Ratings by userid;
Movie_Count = FOREACH Group_By_User GENERATE group as userid, COUNT(Movie_Ratings.$0) as movie_Count;
Movie_Count_Desc = Order Movie_Count by movie_Count DESC;
Top_10_Cus = Limit Movie_Count_Desc 10;
STORE Top_10_Cus INTO '/user/output/pig/top_10_cus_by_number_of_ratings';