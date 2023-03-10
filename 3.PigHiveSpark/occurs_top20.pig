counts = LOAD '/data/googlebooks-eng-all-1gram-20120701-a/googlebooks-eng-all-1gram-20120701-a' as (bigram:chararray, year:chararray, match_count:double, volume_count:int);
grouped_counts = GROUP counts BY bigram;
result = FOREACH grouped_counts GENERATE group, SUM(counts.match_count) / COUNT(counts.match_count);
STORE result INTO '/data/occurrenceAvgNum';
