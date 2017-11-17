from pyspark import SparkConf, SparkContext
import sys, operator
from scipy import *
from scipy.sparse import csr_matrix
from termcolor import colored
"""
 original source :
 https://github.com/Abhishek-Arora/Scalable-Matrix-Multiplication-on-Apache-Spark/blob/master/matrix_multiply_sparse.py
"""
maxRows = 5
maxCols = 5

def createCSRMatrix(input):
	row 	= []
	col 	= []
	data 	= []
	labels 	= []
	rowNumber   = 0

	for lines in input:
		parts = lines.split(' ')
		labels.append(parts[0])

		for itemNumber in range(1,len(parts)):
			splitPair = parts[itemNumber].split(':')
			col.append(int(splitPair[0])-1)
			data.append(int(splitPair[1]))
			row.append(float(rowNumber))
		rowNumber += 1
	csr = csr_matrix((data,(row,col)), shape=(maxRows,maxCols))
	# print colored (csr,"green")
	return csr
	
def multiplyMatrix(csrMatrix):

	csrTransponse = csrMatrix.transpose(copy=True)
	print colored ((csrTransponse*csrMatrix), "cyan")
	return (csrTransponse*csrMatrix)
	
def formatOutput(indexValuePairs):
	return ' '.join(map(lambda pair : str(pair[0]) + ':' + str(pair[1]), indexValuePairs))
		
def main():

	inputFile = "small-data"
	# inputFile = "sample_libsvm_data.txt"
	output 	  = "output.txt"
	conf = SparkConf().setAppName('Sparse_Matrix_Multiplication')
	sc = SparkContext(conf=conf)
	
	dataRDD = sc.textFile(inputFile,2).map(lambda row : row.split('\n'))

	def printF(iterator) : 
		count=0
		for x in iterator:
			# sc.parallelize(x)
			print colored(x,"cyan")
			count=count+1
		print colored(count, "red")

		# print "\n\noutside Loop\n\n"
	
	# sparseMatrix = dataRDD.glom().map(createCSRMatrix)
	# print colored(sparseMatrix, "cyan")

	partList = dataRDD.glom().collect()  # returns a list with list of values in each partition

	partNumber = 0
	for part in partList:
		partNumber += 1
		print colored(partNumber, "red")
		print colored(part, "cyan")

	print colored(sc.parallelize(partList[0]).takeSample(False,), "green")

	# print colored((min(l), max(l), sum(l)/len(l), len(l)), "cyan")  # check if skewed

	# parts = sparseMatrix.repartition(2)

	# sparseMatrix.foreachPartition(lambda x: x.glom() )

	

	# parts.foreachPartitions(lambda x: printF(x))

	# def printP(): print part

	# partitions = sc.parallelize(xrange(0,5))
	# partitions.foreachPartitions( lambda part : printP() )
	# sparseMatrix = sc.textFile(inputFile).map(lambda row : row.split('\n')).map(createCSRMatrix).map(multiplyMatrix).reduce(operator.add)
	
	# out= sc.textFile(inputFile).takeSample(True, 3)
	# rddList = sc.parallelize(out).map(lambda row : row.split('\n'))
	# print colored(rddList,"cyan")
	
	# outputFile = open(output, 'w')
	
	# for row in range(len(sparseMatrix.indptr)-1):
	# 	col = sparseMatrix.indices[sparseMatrix.indptr[row]:sparseMatrix.indptr[row+1]]
	# 	data = sparseMatrix.data[sparseMatrix.indptr[row]:sparseMatrix.indptr[row+1]]
	# 	indexValuePairs = zip(col,data)
	# 	formattedOutput = formatOutput(indexValuePairs)
	# 	outputFile.write(formattedOutput + '\n')
	
if __name__ == "__main__":
	main()