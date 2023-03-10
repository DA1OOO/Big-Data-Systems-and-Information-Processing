create table blog_relation(followee_id string, follower_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ';

create table blog_relation(followee_id string, follower_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ';

create table followee_num as 
select follower_id, count(*) as followee_nums
from blog_relation
group by follower_id;

create table followers as 
select a.followee_id as followee_id, a.follower_id as follower_1, b.follower_id as follower_2
from blog_relation as a join blog_relation as b
on a.followee_id = b.followee_id
where a.follower_id != b.follower_id
;

create table co_followees as
select follower_1, follower_2, collect_list(followers.followee_id) as co_followee, count(*) as co_fol_nums
from followers
group by follower_1, follower_2;

create table blog_pair_similarity as 
select A.follower_1 as follower_1, A.follower_2 as follower_2, A.co_followee as co_followee, (A.co_fol_nums / (B.followee_nums + C.followee_nums - A.co_fol_nums)) as similarity
from co_followees as A
join followee_num as B on A.follower_1 = B.follower_id
join followee_num as C on A.follower_2 = C.follower_id;

create table top3 as 
select * 
from (select * , row_number() over(partition by follower_1 order by similarity desc) as rank from blog_pair_similarity) as a
where a.rank < 4;

create table result as 
select follower_1, follower_2, co_followee, similarity
from top3
where follower_1 like '%2964';