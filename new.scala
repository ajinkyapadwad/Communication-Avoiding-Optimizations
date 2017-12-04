import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{LBFGS, LogisticGradient, SquaredL2Updater}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import scala.io.Source
import org.apache.spark.rdd.RDD

var i = 0
val filename = "sample_libsvm_data.txt"
var thisSeq = Seq[MatrixEntry]()
for (line <- Source.fromFile(filename).getLines) 
{
		// println(line)
		val items = line.split(' ')
		val label = items.head.toDouble

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
val entries = sc.parallelize(thisSeq)
val splits = entries.randomSplit(Array(0.7, 0.3), seed = 11L)
println()
val coordX: CoordinateMatrix = new CoordinateMatrix(entries)
println()
val coordI: CoordinateMatrix = new CoordinateMatrix(splits(1))

println()
entries.collect()
println()
splits(1).collect()
println()
// println(coordX)
// println(coordI)

val X: BlockMatrix = coordX.toBlockMatrix().cache()
val I: BlockMatrix = coordI.toBlockMatrix().cache()

val XI = X.multiply(I)

println(XI)


// val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
// val numFeatures = data.take(1)(0).features.size

// // Split data into training (60%) and test (40%).
// val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)

// // Append 1 into the training data as intercept.
// val training = splits(0).map(x => (x.label, MLUtils.appendBias(x.features))).cache()

// val featureRDD = splits(0).map(x => x.features)
// val labelRDD = splits(0).map(x => x.label)
// val dataRDDF = data.map(x => x.features)
// val dataRDDLabels = data.map(x => x.label)

// println()
// println(featureRDD)
// println()
// println(labelRDD)
// println()
// println(dataRDDF)
// println()
// println(dataRDDLabels)
// println()

// val test = splits(1)

// // Run training algorithm to build the model
// val numCorrections = 10
// val convergenceTol = 1e-4
// val maxNumIterations = 20
// val regParam = 0.1
// val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))

// val (weightsWithIntercept, loss) = LBFGS.runLBFGS(
//   training,
//   new LogisticGradient(),
//   new SquaredL2Updater(),
//   numCorrections,
//   convergenceTol,
//   maxNumIterations,
//   regParam,
//   initialWeightsWithIntercept)

// val model = new LogisticRegressionModel(
//   Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
//   weightsWithIntercept(weightsWithIntercept.size - 1))

// // Clear the default threshold.
// model.clearThreshold()

// // Compute raw scores on the test set.
// val scoreAndLabels = test.map { point =>
//   val score = model.predict(point.features)
//   (score, point.label)
// }

// // Get evaluation metrics.
// val metrics = new BinaryClassificationMetrics(scoreAndLabels)
// val auROC = metrics.areaUnderROC()

// println("Loss of each step in training process")
// loss.foreach(println)
// println("Area under ROC = " + auROC)