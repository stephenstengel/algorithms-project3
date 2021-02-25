#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  project3-stephen-stengel.py
#
#  Stephen Stengel  <stephen.stengel@cwu.edu>  40819903
#  Project3

#Note
#I discovered that spark executes transformations, mapping, and
#reductions, using Lazy execution. It doesn't compute anything until an
#answer is needed, then all the mapping is done at once. So I can't time
#the individual portions of the program-- only the total runtime of each
#word count.
#See: https://stackoverflow.com/questions/36269179/how-to-time-a-transformation-in-spark-given-lazy-execution-style

from pyspark import SparkContext
import re #python regular expressions.
from operator import add
from operator import itemgetter #for sorting by an unnamed key
import time #for timing the reductions.
import os #removing created files
import math #for math.inf


GLOBAL_NUM_PRINT_LINES = 10


def main(args):
	sc = SparkContext()
	print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
	print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
	print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
	print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
	
	
	file1 = "WarIsARacket_djvu.txt"
	file2 = "pride-and-prejudice.txt"
	
	print("Generating temporary text files...")
	file3 = fileMultiplier(file2, 10)
	file4 = fileMultiplier(file2, 100)
	file5 = fileMultiplier(file2, 1000)
	os.system("sync")
	print("Done generating texts!")
	
	#put filenames into array
	fNameArray = [file1, file2, file3, file4, file5]
	# ~ fNameArray = [file1, file2, file3, file4]
	# ~ fNameArray = [file1, file2, file3]
	
	isPrintLists = True
	isPrintTimes = True
	
	# ~ outputTimes = []
	
	print("Starting timing tests...")
	for i in range(0, len(fNameArray) ):
		# ~ outputTimes.append( runTest( fNameArray[i], sc, isPrintLists, isPrintTimes ) )
		runTest( fNameArray[i], sc, isPrintLists, isPrintTimes )
	
	print("Time tests done!")
	
	os.system("rm " + str(file3) + " " + str(file4) + " " + str(file5) )
	
	return 0


#Runs a test of map and reduce on the input file.
#Prints the final word list if isPrintList is True. Prints time if isPrintTime is True.
#Returns the time for execution on each file.
def runTest(fileName, sc, isPrintList, isPrintTime):
	#I run the test five times and take the min time as the output.
	#This makes sense because the input is not random, and the min time will be without other programs messing it up.
	
	mintime = math.inf
	outlist = None
	for i in range(0,5):
		thisList, thisTime = capWordCounter(fileName, sc)
		# ~ print("test " + str(i) + " of\t" + fileName + "time:\t" + str(thisTime))
		if thisTime < mintime:
			outlist = thisList
			mintime = thisTime
	
	if isPrintList or isPrintTime:
		if isPrintTime:
			print("Time for " + fileName + "\t: " + str(mintime))
		if isPrintList:
			print("List for " + fileName + "...")
			printOutput(outlist, GLOBAL_NUM_PRINT_LINES)

	return mintime


#Makes a count of the number of each capitalized word in an input text.
#Returns a list and execution time. Params: Input file, spark context.
def capWordCounter(myFile, sc):
	allStart = time.time()
	theText = sc.textFile(myFile)
	
	#NMy old method for getting the words in two steps.
	# ~ allWords = theText.flatMap(lambda line: line.split(" "))
	# ~ #remove words that are not capitalized.
	# ~ capWords = allWords.flatMap(capMapper)
	
	#This method finds all words that match an expression. That expression looks for capitalized words. It's faster and more accurate.
	capWords = theText.flatMap( lambda line: re.findall( r"\b[A-Z][a-zA-Z]*\b", line ) )

	#Assign each word the value of 1.
	wordMap = capWords.map(lambda word: (word, 1))
	
	#Reduce the word list by summing the separate occurrences.
	wordCounts = wordMap.reduceByKey(add)
	
	wordCountsList = wordCounts.collect()  #collect turns it into a list.
	
	allEnd = time.time()
	
	return wordCountsList, (allEnd - allStart)


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

#Prints the output of reduction. Max ten lines.
def printOutput(wordCountsList, maxNumLines):
	count = 1
	for cWord in sorted(wordCountsList, key=itemgetter(1), reverse=True):
		if count > maxNumLines:
			break
		firstItemGet = itemgetter(0)
		secondItemGet = itemgetter(1)
		print( str(firstItemGet(cWord)) + "\t" + str(secondItemGet(cWord)) )
		count += 1

#Creates a file a multiple larger than input file by rewiting lines.
#Returns the name of the file.
def fileMultiplier(inFileName, mult):
	if mult < 2:
		mult = 2
	outFileName = inFileName + "-" + str(mult) + ".txt"
	with open(inFileName, 'r') as infile, open(outFileName, 'w') as outfile:
		for line in infile:
			for i in range(0, mult):
				outfile.write(line)

	return outFileName


if __name__ == '__main__':
	import sys
	sys.exit(main(sys.argv))
