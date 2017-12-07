import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import scala.io.Source
import org.apache.spark.rdd.RDD

val tick = System.currentTimeMillis()	// start time

// ------------------------------------------------------------------------------------------------
/*
Spark's LoadLibSvmfile() is usually used for this purpose,
but it creates a LabelledPoint format and we need 
separate matrices for labels and values, hence manual read :
*/

var row = 0									// row number 
val filename = "sample_libsvm_data.txt"		// input data
var thisSeq = Seq[MatrixEntry]()			// sequence of MatrixEntries
var labelSeq = Seq[MatrixEntry]()			// sequence of MatrixEntries for Labels

// read line by line and split into parts
for (line <- Source.fromFile(filename).getLines) 
{
	val items = line.split(' ')
	val label = items.head.toDouble
	val thisLabelEntry = MatrixEntry(row, 0, label)
	labelSeq = labelSeq :+ thisLabelEntry

	for (para <- items.tail)
	{
		val indexAndValue = para.split(':')
		val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
		val value = indexAndValue(1).toDouble
		val thisEntry = MatrixEntry(row, index, value)
		thisSeq = thisSeq :+ thisEntry
	}
	row = row + 1
}

// ------------------------------------------------------------------------------------------------

// create RDD from the MatrixEntries
// upto 20 : ~11 seconds, 200 : ~4 seconds
val numPartitions = 200
val values 	 = sc.parallelize(thisSeq, numPartitions)
val labels = sc.parallelize(labelSeq, numPartitions)

val numFeatures  = 9
// val dv: Vector = Vectors.zeros(numFeatures)

// parameters
val t = 100
val s = 5

// iterators
var p = 0
var q = 0

// declare temporary RDDs for MatrixEntries of product values
var productEntries: RDD[MatrixEntry]= null
var productListRDDs = Seq[RDD[MatrixEntry]]()

// ------------------------------------------------------------------------------------------------

// CA-SFISTA algorithm-
for ( p <- 0 to t/s )
{
	for ( q <- 0 to s )
	{	
		// randomized sampling of values and labels
		var sampledValues 	 = values.sample(false, 0.2, 1L).collect()
		var sampledValuesRDD = sc.parallelize(sampledValues)
		var sampledLabels 	 = labels.sample(false, 0.2, 1L).collect()
		var sampledLabelsRDD = sc.parallelize(sampledLabels)

		// create sparse matrices from the RDDs
		var coordX: CoordinateMatrix = new CoordinateMatrix(sampledValuesRDD)
		var coordY: CoordinateMatrix = new CoordinateMatrix(sampledLabelsRDD)

		var rddX = coordX.entries.map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
		var rddY = coordY.entries.map({ case MatrixEntry(j, k, w) => (j, (k, w)) })

		// create RDD of MatrixEntries after with index 
		productEntries = rddX
			.join(rddY)
			.map({ case (_, ((i, v), (j, w))) => MatrixEntry(i, j, (v * w)) })			
			// 	.reduceByKey(_ + _)
			// .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })
		productListRDDs = productListRDDs :+ productEntries	
	}

	for (entry <- productListRDDs) {
		// println(entry)
		entry.map({ case MatrixEntry(i, j, v) => ((i, j), v) })
			.reduceByKey(_ + _)
			.map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })

	}

}

// val tock = System.nanoTime()
val tock = System.currentTimeMillis()

println("Elapsed time: " + (tock - tick)/1000F + " seconds")
// println(productListRDDs)
// var coo = new CoordinateMatrix(productEntries)
// val X: BlockMatrix = coo.toBlockMatrix()
// val local2 = X.toLocalMatrix()