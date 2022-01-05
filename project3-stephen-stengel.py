#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  project3-stephen-stengel.py
#
#  Stephen Stengel  <stephen.stengel@cwu.edu>  40819903
#  Project3

#Note
#I run this script using: python3 project3-stephen-stengel.py


from pyspark import SparkContext
from pyspark import SparkConf
import re #python regular expressions.
from operator import add
from operator import itemgetter #for sorting by an unnamed key
import time #for timing the reductions.
import os #removing created files
import math #for math.inf
import matplotlib.pyplot as plt #for plotting


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
	
	print("Starting timing tests...")
	for i in range(0, len(fNameArray) ):
		runTest( fNameArray[i], sc, isPrintLists, isPrintTimes )
	print("Time tests done!")
	
	
	print("Starting thread tests...")
	threadMins = []
	for i in range(1, 9):
		sc.stop()
		conf = SparkConf()
		conf.setMaster("local[" + str(i) + "]")
		sc = SparkContext(conf=conf)
		print("Testing with " + str(i) + " thread...")
		thisMin = runTest( file5, sc, isPrintLists, isPrintTimes )#####
		threadMins.append( thisMin )
	print("Saving thread graph...")
	printThreadGraph( range(1, len(threadMins) + 1), threadMins)
	print("Done!")
	
	print("Removing temporary files...")
	os.system("rm " + str(file3) + " " + str(file4) + " " + str(file5) )
	print("Done!")
	
	return 0


#Runs a test of map and reduce on the input file.
#Prints the final word list if isPrintList is True. Prints time if isPrintTime is True.
#Returns the min time for execution on the file.
def runTest(fileName, sc, isPrintList, isPrintTime):
	#I run the test five times and take the min time as the output.
	#This makes sense because the input is not random, and the min time will be without other programs messing it up.
	
	mintime = math.inf
	outlist = None
	for i in range(0,5):
		thisList, thisTime = capWordCounter(fileName, sc)
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
#Unused.
def capMapper(myWord):
	if isCapWord(myWord):
		return [myWord]
	else:
		return []


#Returns true if the input word string is capitalized in the first character.
#Unused.
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


# ~ def printThreadGraph(xArray, yArray, xlow, xhigh, ylow, yhigh):
def printThreadGraph(xArray, yArray):
	plt.title("Runtime of map/reduce on pride-and-prejudice-1000.txt")
	plt.xlabel("Number of Threads")
	plt.ylabel("Time in seconds")
	plt.scatter(xArray, yArray, color="Red")
	# ~ plt.xlim(xlow, xhigh)
	# ~ plt.ylim(ylow, yhigh)
	
	plt.savefig("thread-plot.png")
	
	plt.clf()


if __name__ == '__main__':
	import sys
	sys.exit(main(sys.argv))
