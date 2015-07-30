/*******************************************************

Tweets
Steven Lin

This scripts takes a file (or folder) of tweets and 
files with good and bad words, and outputs the number of 
tweets with positive sentiment and the number of tweets 
with negative sentiment

@ param: 
	input_hdfs_data: assignment4/tweets/data (file or directory)
	input_hdfs_good: assignment4/tweets/dictionary/good.txt
	input_hdfs_bad:  assignment4/tweets/data/dictionary/bad.txt
	output_hdfs:     assignment4/tweets/output (directory)

@ output: 
	folder output_hdfs with mapreduce job part files
	
To call script: 

pig -param input_hdfs_data ='assignment4/tweets/testdata' -param... lin_tweets.pig 

Overall steps:
1) Get column of tweets, assign unique id to each tweet, remove punctuation and tokenize words
2) Convert to lower case, remove punctuation, remove leading and trailing whitespace
3) Give a +1 score for good words and -1 score for bad words
4) Join tweets with dictionary with scores of good and bad words
5) Group words by tweet id, compute sentiment of each tweet by adding the scores of all its words.
   The sentiment score of a tweet is defined as: the number of good words in the tweet – the number of
   bad words in the tweet. A tweet
6) Classify each tweet as positive, negative or neutral sentiment
   A tweet has a positive sentiment if the sentiment score is great than 0 and
   negative if it is less than 0.
7) Group tweets by sentiment, discard neutral sentiment (since tweet might have all words not in dicitonary)
8) Count number of tweets with positive sentiment and the number of tweets with negative sentiment

**********************************************************/

--data = LOAD 'assignment4/tweets/testdata' USING PigStorage ('|') AS (f1,f2,f3,f4,f5,f6,f7,f8,f9,tweet: chararray);
--good = LOAD 'assignment4/tweets/dictionary/good.txt' AS (word: chararray);
--bad = LOAD  'assignment4/tweets/dictionary/bad.txt' AS (word: chararray);


-- load data and dictionary
data = LOAD '$input_hdfs_data' USING PigStorage ('|') AS (f1,f2,f3,f4,f5,f6,f7,f8,f9,tweet: chararray);
good = LOAD '$input_hdfs_good' AS (word: chararray);
bad = LOAD  '$input_hdfs_bad' AS (word: chararray);

-- select only column with tweet
-- tweets: {tweet: chararray}
tweets = FOREACH data GENERATE tweet;         

--tweets2 = FILTER tweets1 BY tweet IS NOT NULL;  -- not working, messes up later

-- add unique id to tweet  
-- tweets: {rank_tweets: long,tweet: chararray}
tweets = rank tweets;                            

-- replace punctuation except dash with blank space
-- case: 'wow!!ok' should be 'wow ok'
-- case: 'little-known' should be 'little-known'
-- tweets: {id: long,tweet: chararray}
tweets = FOREACH tweets GENERATE rank_tweets as id , REPLACE(tweet,'[^A-Za-z0-9-]',' ') as tweet:chararray;

-- TRIM: remove leading and trailing whitespace 
-- LOWER: lower case (e.g. Wow won't match wow in dictionary )
-- TOKENIZE: {(id=A: {word1, word2...})}
-- FLATTEN: {(id=A,word1 ), (id=A, word2)....} ...(a, {(b,c), (d,e)}) -> Flatten -> (a,b,c), (a,d,e)
-- http://stackoverflow.com/questions/17951375/what-exactly-am-i-doing-wrong-with-my-wordcount-program-pig
-- tweets: {id: long,word: chararray}
tweets = FOREACH tweets GENERATE id , FLATTEN(TOKENIZE(TRIM(LOWER(tweet)))) as word:chararray;

-- assign scores and combine in one relation
-- scores: {word: chararray,score: int}
good = FOREACH good GENERATE word, 1 as score;
bad = FOREACH bad GENERATE word, -1 as score;
scores = UNION good, bad;

-- inner join and subset {id, score}
-- wordScores: {tweets::id: long,scores::score: int}
wordScores = JOIN tweets BY word, scores BY word USING 'replicated';
wordScores = FOREACH wordScores GENERATE id, score;

-- group by id {id=A, (score1, score2...), id=B....}
-- wordScoresByID: {group: long,wordScores: {(tweets::id: long,scores::score: int)}}
wordScoresByID = GROUP wordScores BY id;

-- compute sentiment {id =A , score = scoreA}
-- tweetScoresByID: {id: long,score: long}
tweetScoresByID = FOREACH wordScoresByID GENERATE group as id, SUM(wordScores.score) as score;

-- Classify tweet sentiment as positive or negative {id = A, sentiment = sentimentA }
-- http://pig.apache.org/docs/r0.7.0/piglatin_ref2.html#Arithmetic+Operators+and+More
-- sentimentByID: {id: long,sentiment: chararray}
sentimentByID = FOREACH tweetScoresByID GENERATE id,  (score>0 ? 'Positive' :
                                                      (score<0 ? 'Negative' : 
                                                                 'Neutral')) as sentiment;
-- group id's by sentiment {Positive, {idA, idB...}}
-- remove neutral (because neutral set does not include case where tweet had neither positive nor negative words)
-- groupSentiments: {group: chararray,sentimentByID: {(id: long,sentiment: chararray)}}
groupSentiments = GROUP sentimentByID BY sentiment;
groupSentiments = FILTER groupSentiments by group != 'Neutral';

-- number of tweets with positive sentiment and the number of tweets with negative sentiment
-- {Positive: 50, Negative: 30}
-- countSentiments: {group: chararray,long}
countSentiments = FOREACH groupSentiments GENERATE group, COUNT(sentimentByID);

STORE countSentiments INTO '$output_hdfs' USING PigStorage('\t');
