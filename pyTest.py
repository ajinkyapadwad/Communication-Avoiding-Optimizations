from pyspark import SparkConf, SparkContext
import sys, operator
from scipy import *
from scipy.sparse import csr_matrix

"""
 original source :
 https://github.com/Abhishek-Arora/Scalable-Matrix-Multiplication-on-Apache-Spark/blob/master/matrix_multiply_sparse.py
"""

def createCSRMatrix(input):
	row 	= []
	col 	= []
	data 	= []
	labels 	= []
	count   = 0

	for lines in input:
		parts = lines.split(' ')
		labels.append(parts[0])

		for itemNumber in range(1,len(parts)):
			splitPair = parts[itemNumber].split(':')
			col.append(int(splitPair[0]))
			data.append(int(splitPair[1]))
			row.append(float(count))
		count += 1
	
	return csr_matrix((data,(row,col)), shape=(1,100))
	

def multiplyMatrix(csrMatrix):

    csrTransponse = csrMatrix.transpose(copy=True)

    return (csrTransponse*csrMatrix)
	
def formatOutput(indexValuePairs):
	return ' '.join(map(lambda pair : str(pair[0]) + ':' + str(pair[1]), indexValuePairs))
		

def main():

	
	input = "small-data"
	output = "output.txt"
	
	
	conf = SparkConf().setAppName('Sparse Matrix Multiplication')
	sc = SparkContext(conf=conf)
	assert sc.version >= '1.5.1'
	
	sparseMatrix = sc.textFile(input).map(lambda row : row.split('\n')).map(createCSRMatrix).map(multiplyMatrix).reduce(operator.add)
	outputFile = open(output, 'w')
	
	for row in range(len(sparseMatrix.indptr)-1):
		col = sparseMatrix.indices[sparseMatrix.indptr[row]:sparseMatrix.indptr[row+1]]
		data = sparseMatrix.data[sparseMatrix.indptr[row]:sparseMatrix.indptr[row+1]]
		indexValuePairs = zip(col,data)
		formattedOutput = formatOutput(indexValuePairs)
		outputFile.write(formattedOutput + '\n')
		

	
if __name__ == "__main__":
	main()
