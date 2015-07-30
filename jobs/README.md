# Job

* Run:
	```shell
	python lin_jobs.py
	```

* Data: There are several files containing job descriptions (from a job advertising web company). There is one job description per line with ‘\n’ as the delimiter and it is included in the second field. The first field includes a unique job id. All necessary files are in folder /home/public/course/pig/jobs.

* The goal is to provide files with a single row per job ad (thus in total the same number of records as the input data files). The order of the words in the final file for a given ad doesn’t matter.

* For each row the following tasks must were processed in the same order:
	1. Tokenize
	2. Remove all stop words (these are words such as ‘a,the,in,of,...’). All stop words are provided in the file stopwords-en.txt
	3. To all remaining words apply stemming (change ‘badly’ to ‘bad,’ ‘going’ to ‘go,’ ...). An
	implementation is provided in the java file Porter.java.
	4. Correct the misspelled words. The list of all English root words is available in dictionary.txt.Given a word, if it is available in the dictionary, it is spelled correctly. If it is not available, then compute the Levenshtein distance with all the words in the dictionary and select the one with the lowest distance. An implementation is provided in the java file Levenshtein.java.