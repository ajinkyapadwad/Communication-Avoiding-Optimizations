import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import scala.io.Source
import org.apache.spark.rdd.RDD


	var i = 0
	val filename = "sample_libsvm_data.txt"
	var thisSeq = Seq[MatrixEntry]()
	var labelSeq = Seq[MatrixEntry]()

	for (line <- Source.fromFile(filename).getLines) 
	{
			// println(line)
			val items = line.split(' ')
			val label = items.head.toDouble
			val thisLabelEntry = MatrixEntry(i, 0, label)
			labelSeq = labelSeq :+ thisLabelEntry

			for (para <- items.tail)
			{
				val indexAndValue = para.split(':')
				val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
				val value = indexAndValue(1).toDouble
				val thisEntry = MatrixEntry(i, index, value)
				thisSeq = thisSeq :+ thisEntry
			}
			i = i + 1
	}

	val numFeatures = 9
	val entries = sc.parallelize(thisSeq)
	// val allEntries = entries.collect()
	val labelEntries = sc.parallelize(labelSeq)
	// val dv: Vector = Vectors.zeros(numFeatures)
	var out = null

	//initialize vars
	val t = 100
	val s = 10
	var p = 0
	var q = 0

	val tick = System.currentTimeMillis()

	for ( p <- 0 to t/s )
	{
		for ( q <- 0 to s )
		{
			var samples = entries.sample(false, 0.2, 1L).collect()
			var sampleRDD = sc.parallelize(samples)

			var labelSamples = labelEntries.sample(false, 0.2, 1L).collect()
			var labelSampleRDD = sc.parallelize(labelSamples)

			var coordX: CoordinateMatrix = new CoordinateMatrix(sampleRDD)
			// var coordY: CoordinateMatrix = new CoordinateMatrix(labelSampleRDD)
			var coordY: CoordinateMatrix = coordX.transpose

			var rddX  = coordX.entries.map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
			var rddXT = coordY.entries.map({ case MatrixEntry(j, k, w) => (j, (k, w)) })

			// var multFactor = s * numFeatures
			var productEntries = rddX.join(rddXT).map({ case (_, ((i, v), (k, w))) => ((i,k), (v * w)) }).reduceByKey(_ + _).map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })
			productEntries.collect()
		}
		
		
		// var productEntriesRDD = sc.parallelize(productEntries) // productEntries is Array of MatrixEntry
		var mat = new CoordinateMatrix(productEntries)
		var X: BlockMatrix = mat.toBlockMatrix().cache()
		var local2 = X.toLocalMatrix()


		// // // Get its size.
		// var out = mat.entries.map({ case MatrixEntry(i, j, v) => Array(i,j,v).mkString(",")}).collect().foreach(println)
		// val m = mat.numRows()
		// val n = mat.numCols()

	}

	val tock = System.currentTimeMillis()
	println("Elapsed time: " + (tock - tick)/1000F + " seconds")