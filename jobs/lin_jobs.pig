/*******************************************************
Assignment 4
Authors: Stenve Lin

Pig script 1) Tokenizes, 2) Removes Stop Words 
           3) Stms 4) Spell checks

Input files:
- Data: job descriptions
- Dictionary
- Stop word

Output files:
- File with keywords by job ID

Param:
$input_hdfs_data
$input_hdfs_stop
$output_hdfs

Assumes dictionary.txt located in home dir HDFS

UDF:
lin_jobs_stem
lin_jobs_correctWord

**********************************************************/

-- register UDFs
REGISTER lin_jobs_stem.jar;
REGISTER lin_jobs_correctWord.jar;

-- read data
--data = LOAD 'assignment4/jobs/testdata' USING PigStorage (',') AS (id: chararray,job: chararray);
--stopWords = LOAD 'assignment4/jobs/stopwords' AS (word: chararray);
data = LOAD '$input_hdfs_data' USING PigStorage (',') AS (id: chararray,job: chararray);
stopWords = LOAD '$input_hdfs_stop' AS (word: chararray);

-- Step 1: Tokenize ***************************************************************************************

-- remove puncuation except ' and - (e.g. "won't" and "little-known" want to keep to match with stop words)
-- replace by space so can correctly tokenize

-- TRIM: remove leading and trailing whitespace 
-- LOWER: lower case (e.g. Wow won't match wow in stop words )
-- TOKENIZE: {(id=A: {word1, word2...})}
-- FLATTEN: {(id=A,word1 ), (id=A, word2)....} ...(a, {(b,c), (d,e)}) -> Flatten -> (a,b,c), (a,d,e)
-- http://stackoverflow.com/questions/17951375/what-exactly-am-i-doing-wrong-with-my-wordcount-program-pig
-- ads: {id: long,word: chararray}
ads = FOREACH data GENERATE id , FLATTEN(TOKENIZE(TRIM(LOWER(REPLACE(job,'[^A-Za-z0-9-\']',' '))))) as word:chararray;

-- Step 2: Remove Stop Words *********************************************************************************

-- left outer join (will have everthing in ads + matches with stopWords)
-- for example (id, and, and) --> don't want this, and (id, analytics, ) --> want this
-- joined:  {ads::id: chararray,ads::word: chararray,stopWords::word: chararray}
joined = JOIN ads BY word LEFT, stopWords BY word USING 'replicated';

-- remove matches with stopwords
-- {ads::id: chararray,ads::word: chararray,stopWords::word: chararray}
filtered = FILTER joined BY stopWords::word IS NULL;

-- select relevant fields
-- {id: chararray,word: chararray}
filtered = FOREACH filtered GENERATE ads::id as id, ads::word as word;

-- Step 3: Stemming *********************************************************************************

-- NOTE: Change methods in Porter class to Public
-- NOTE: following order in the assignment (e.g stemming then spell check) which might not be
-- a good idea because a word like require will become requir, which is not in 
-- dictionary so will have to match it with the word require in dictionary

stemmed = FOREACH filtered GENERATE id, lin_jobs_stem(word) as word:chararray;

-- Step 4: Correct Spelling *************************************************************************

corrected = FOREACH stemmed GENERATE id, lin_jobs_correctWord(word) as word:chararray;

-- correctedByJob: {group: chararray,corrected: {(id: chararray,word: chararray)}}
correctedByJob  = GROUP corrected BY id;

-- correctedByJob: {id: chararray,word: {(word: chararray)}}
correctedByJob  = FOREACH correctedByJob GENERATE group as id, corrected.word as word;

STORE correctedByJob INTO '$output_hdfs' USING PigStorage('\t');