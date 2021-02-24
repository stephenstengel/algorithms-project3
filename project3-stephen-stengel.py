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
from operator import itemgetter #for sorting by an unnamed key


def main(args):
	print("Hi lol")
	
	myFile = "file:///home/stephen/Documents/School/WarIsARacket_djvu.txt"  
	sc = SparkContext()
	
	theText = sc.textFile(myFile)
	allWords = theText.flatMap(lambda line: line.split(" "))
	
	#capMapper() removes words that are not capitalized.
	capWords = allWords.flatMap(capMapper)

	#Assign each word the value of 1.
	wordMap = capWords.map(lambda word: (word, 1))
	
	#Reduce the word list by adding the separate occurrences. The count of each word remains.
	wordCounts = wordMap.reduceByKey(add).collect()  #collect turns it into a list.
	# ~ wordCounts = wordMap.reduceByKey(add)
	
	# ~ print(sorted(wordCounts))
	
	# save the counts to output
	# ~ wordCounts.saveAsTextFile("out")
	
	for cWord in sorted(wordCounts, key=itemgetter(1), reverse=True):
		firstItemGet = itemgetter(0)
		secondItemGet = itemgetter(1)
		if secondItemGet(cWord) > 5:
			print( str(firstItemGet(cWord)) + "\t" + str(secondItemGet(cWord)) )

	
	return 0

#For use in flatMap(). Returns the word only if Capitalized. Else nothing.
def capMapper(myWord):
	if isCapWord(myWord):
		return [myWord]
	else:
		return []

#Returns true if the input word string is capitalized in the first character.
def isCapWord(myWord):
	myRegex = re.compile("^[A-Z]")
	if myRegex.match(myWord):
		return True
	else:
		return False

if __name__ == '__main__':
	import sys
	sys.exit(main(sys.argv))
