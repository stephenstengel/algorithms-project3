#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  project3-stephen-stengel.py
#
#  Stephen Stengel  <stephen.stengel@cwu.edu>  40819903
#  Project3

from pyspark import SparkContext
import re #python regular expressions.
from operator import add


def main(args):
	print("Hi lol")
	
	myFile = "file:///home/stephen/Documents/School/WarIsARacket_djvu.txt"  
	sc = SparkContext()
	
	theText = sc.textFile(myFile)
	allWords = theText.flatMap(lambda line: line.split(" "))
	
	#What I need to do now is create a new rdd containing only words that start with capital letters. ^[A-Z]
	capWords = allWords.flatMap(capMapper)
	

	# count the occurrence of each word
	wordMap = capWords.map(lambda word: (word, 1))
	
	# ~ wordCounts = wordMap.reduceByKey(lambda a,b:a +b)
	wordCounts = wordMap.reduceByKey(add).collect()  #collect turns it into a list.
	# ~ wordCounts = wordMap.reduceByKey(add)
	
	print(sorted(wordCounts))
	
	# save the counts to output
	# ~ wordCounts.saveAsTextFile("out")

	
	# reg exp I want is ^[A-Z]
	# "first letter of string is A-Z. It's already split by words now so will work
	
	# ~ print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
	
	if isCapWord("Bear"):
		print("cap")
	else:
		print("nocap")
		
	return 0

def capMapper(myWord):
	if isCapWord(myWord):
		return [myWord]
	else:
		return []

def isCapWord(myWord):
	myRegex = re.compile("^[A-Z]")
	if myRegex.match(myWord):
		return True
	else:
		return False

if __name__ == '__main__':
	import sys
	sys.exit(main(sys.argv))
