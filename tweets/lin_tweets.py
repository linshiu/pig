# -*- coding: utf-8 -*-
"""
Python scripts calls lin_tweets.pig and puts output in local directory

@author: steven
"""

import argparse
import os
import sys

# parameters
script = 'lin_tweets.pig'
input_hdfs_data= 'assignment4/tweets/data'
#input_hdfs_data= 'assignment4/tweets/testdata'
#input_hdfs_data= 'assignment4/tweets/data/tweets_20121102.txt'
input_hdfs_good= 'assignment4/tweets/dictionary/good.txt'
input_hdfs_bad=  'assignment4/tweets/dictionary/bad.txt'
output_hdfs=     'assignment4/tweets/output'
outFileName=     'lin_tweets.txt'

# remove output folder in hadoop
os.system('hadoop fs -rm -r {output}'.format(output = output_hdfs))

# call pig script
os.system("pig -param input_hdfs_data={input} "
	      "-param input_hdfs_good={good} "
	      "-param input_hdfs_bad={bad} "
	      "-param output_hdfs={output} "
	      "{script}".format(input=input_hdfs_data, good = input_hdfs_good, bad = input_hdfs_bad, output = output_hdfs, script= script ))

# combine outputs to local
os.system("hadoop fs -getmerge {output} {outputFile}".format(output = output_hdfs, outputFile= outFileName))

# remove crc file http://stackoverflow.com/questions/15434709/checksum-exception-when-reading-from-or-copying-to-hdfs-in-apache-hadoop
os.system("rm .{outputFile}.crc".format(outputFile= outFileName))

print("....created file {outputFile} in local directory".format(outputFile= outFileName))
