in_d = LOAD '$input' AS (video_id:chararray, uploader:chararray, age:int, category:chararray, length:int, views:chararray, rate:chararray, ratings:float, comments:chararray, related_ids);
mapped = FOREACH in_d GENERATE rate, ratings, video_id;
-- DUMP mapped;
-- groupped_mapped = GROUP mapped BY rate;
groupped_mapped = GROUP mapped BY STRSPLIT(rate, '.', 2);
groupped_mapped = GROUP mapped BY SUBSTRING( STRSPLIT(rate, '.', 2).$1, 1, ((int) SIZE( ((chararray) STRSPLIT(rate, '.', 2).$1) )) );
-- filtered_mapped = FOREACH groupped_mapped GENERATE SUBSTRING(group.$1, 1, ((int) SIZE(group.$1)), TOBAG(mapped.rate, mapped.video_id));
-- DUMP groupped_mapped;
REGISTER $piglig/piggybank.jar;
DEFINE MultiStorage org.apache.pig.piggybank.storage.MultiStorage('$output', '0', 'none', '\\t');
STORE groupped_mapped INTO '$output' USING MultiStorage('$output', '0', 'none', '\\t');