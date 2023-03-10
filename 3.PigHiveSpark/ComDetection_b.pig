-- Step 1 加载并求出笛卡尔积
blog = LOAD '/data/medium/medium_relation' USING PigStorage(' ')  AS (followee:chararray, follower:chararray);
blog_grpd = GROUP blog BY followee;
blog_grpd_dbl = FOREACH blog_grpd GENERATE group, blog.follower AS blog1, blog.follower AS blog2;
cofollow = FOREACH blog_grpd_dbl GENERATE group, FLATTEN(blog1) as blog1, FLATTEN(blog2) as blog2;
cofollow_filtered = FILTER cofollow BY blog1 < blog2;

-- 求出每个blog关注对象的个数
grpd_blog = GROUP blog BY follower;
followee_count = FOREACH grpd_blog GENERATE group AS blog, COUNT(blog.followee) AS followee;

-- Step 2 汇总共同关注的blog及数目
grpd_cofollow_filtered = GROUP cofollow_filtered BY (blog1, blog2);
co_followee = FOREACH grpd_cofollow_filtered GENERATE FLATTEN(group), cofollow_filtered.group AS co_followees, COUNT(cofollow_filtered) AS co_nums; 

-- 求similarity
CC = JOIN co_followee BY blog1, followee_count BY blog;
DD = JOIN CC BY blog2, followee_count BY blog;
EE = FOREACH DD GENERATE $0 AS blog1, $1 AS blog2, $2 AS co_followees, ((DOUBLE)$3 / (DOUBLE)($5 + $7 - $3)) AS similarity;

-- 根据左侧找出top3
FF = GROUP EE BY blog1;
top3_left = FOREACH FF {
	sorted = ORDER EE BY similarity DESC;
	top = LIMIT sorted 3;
	GENERATE group, FLATTEN(top);
};

-- 根据右侧找出top3
GG = GROUP EE BY blog2;
top3_right = FOREACH GG {
	sorted = ORDER EE BY similarity DESC;
	top = LIMIT sorted 3;
	GENERATE group, FLATTEN(top);
};

-- 合并之后可能会超过三个
topN = UNION top3_left, top3_right;

-- 再次取top3个
grpd_topN = GROUP topN BY group;
top3 = FOREACH grpd_topN {
	sorted = ORDER topN BY similarity DESC;
	top = LIMIT sorted 3;
	GENERATE FLATTEN(top);
};

format_result = FOREACH top3 GENERATE group, CONCAT(CONCAT(blog1,':'),blog2) AS blog_pair, co_followees, similarity;

-- 过滤结果
result = FILTER top3 BY group matches '.*2964';
STORE result INTO '/data/top3_co_followees';

dump result;