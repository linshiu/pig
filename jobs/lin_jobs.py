# -*- coding: utf-8 -*-
"""
Python scripts calls lin_jobs.pig and puts output in local directory
Assumes dictionary.txt file located in user's home directory in HDFS

@author: steven
"""

import argparse
import os
import sys

# parameters
script = 'lin_jobs.pig'
#input_hdfs_data= 'assignment4/jobs/data'
input_hdfs_data= 'assignment4/jobs/testdata'
input_hdfs_stop= 'assignment4/jobs/stopwords'
output_hdfs=     'assignment4/jobs/output'
outFileName=     'lin_jobs.txt'

# remove output folder in hadoop
os.system('hadoop fs -rm -r {output}'.format(output = output_hdfs))

# call pig script
os.system("pig -param input_hdfs_data={input} "
	      "-param input_hdfs_stop={stop} "
	      "-param output_hdfs={output} "
	      "{script}".format(input=input_hdfs_data, stop= input_hdfs_stop, output = output_hdfs, script= script ))

# combine outputs to local
os.system("hadoop fs -getmerge {output} {outputFile}".format(output = output_hdfs, outputFile= outFileName))

# remove crc file http://stackoverflow.com/questions/15434709/checksum-exception-when-reading-from-or-copying-to-hdfs-in-apache-hadoop
os.system("rm .{outputFile}.crc".format(outputFile= outFileName))

print("....created file {outputFile} in local directory".format(outputFile= outFileName))
