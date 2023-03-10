-- Step 1 加载并求出笛卡尔积
blog = LOAD '/data/medium/medium_relation' USING PigStorage(' ')  AS (followee:chararray, follower:chararray);
blog_grpd = GROUP blog BY followee;
blog_grpd_dbl = FOREACH blog_grpd GENERATE group, blog.follower AS blog1, blog.follower AS blog2;
cofollow = FOREACH blog_grpd_dbl GENERATE group, FLATTEN(blog1) as blog1, FLATTEN(blog2) as blog2;
cofollow_filtered = FILTER cofollow BY blog1 < blog2;
-- Step 2 汇总共同关注的blog及数目
grpd_cofollow_filtered = GROUP cofollow_filtered BY (blog1, blog2);
co_followee = FOREACH grpd_cofollow_filtered GENERATE FLATTEN(group), cofollow_filtered.group AS co_followees, COUNT(cofollow_filtered) AS co_nums; 
-- Step 3
-- 根据左侧blog找出其最大共同关注数
grpd_cofollowee_left = GROUP co_followee BY blog1;
max_co_follower_left = FOREACH grpd_cofollowee_left GENERATE group, MAX(co_followee.co_nums) AS max_nums;
-- dump max_co_follower_left;

-- 根据右侧blog找出其最大共同关注数
grpd_cofollowee_right = GROUP co_followee BY blog2;
max_co_follower_right = FOREACH grpd_cofollowee_right GENERATE group, MAX(co_followee.co_nums) AS max_nums;
-- dump max_co_follower_right;

-- Step 4
-- 连接左右两表后，取最大值,找到了单个博客最大共同关注的个数
mix_max = UNION max_co_follower_left, max_co_follower_right;
grpd_mix_max = GROUP mix_max BY group;
union_mix_max = FOREACH grpd_mix_max GENERATE group AS blog, MAX(mix_max.max_nums) as max_co_fol;
-- dump union_mix_max;

-- Step 5
-- 将最大值表和co_followee表进行合并，然后将不是最大值的组合进行过滤
C = JOIN union_mix_max BY blog, co_followee BY blog1; 
D = FILTER C BY max_co_fol == co_nums;
-- dump D;

E = JOIN union_mix_max BY blog, co_followee BY blog2;
F = FILTER E BY max_co_fol == co_nums;
-- dump F;

G = UNION D, F;

-- Step 6
-- 按固定格式输出
H = FOREACH G GENERATE blog, CONCAT(CONCAT(blog1,':'),blog2) AS blog_pair, co_followees, co_nums;
I = FILTER H BY blog matches '.*2964';
dump I;
--STORE I INTO '/data/co_followees';