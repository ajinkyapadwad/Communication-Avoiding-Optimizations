import org.apache.spark.mllib.util.MLUtils
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector, DenseVector}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import scala.io.Source
import scala.collection.mutable.ListBuffer

def toBreeze(v: Vector): BV[Double] = v match {
  case DenseVector(values) => new BDV[Double](values)
  case SparseVector(size, indices, values) => {
    new BSV[Double](indices, values, size)
  }
}

// --------------------------------------------------------------------------
// val data = MLUtils.loadLibSVMFile(sc, "sample_libsvm_data.txt")
// val data1 = MLUtils.loadLibSVMFile(sc, "small-data")
println
val data2 = sc.textFile("small-data")
var i=0
// val filename = "fileopen.scala"

var row 	= new Array[Int](12)
var col 	= new Array[Int](12)
var values 	= new Array[Double](12)

var rowIndex = 0
var counter = 0
for (line <- Source.fromFile("small-data").getLines) {
	// println("LINE : ", line)
    val arr=line.split(" ")
    val label =arr(0)
   	// print("Label:", label," ")
    for( i<-1 until arr.length)
    {

    	val arr2= arr(i).split(":")
    	// val index=arr2(0)
    	col(counter) =  arr2(0).toInt
    	// val value=arr2(1)
    	values(counter) =  arr2(1).toInt
    	row(counter) =  rowIndex
    	counter = counter +1 
    }   
    rowIndex += 1

}
var i=0
for(i<- 0 to row.length){
	println("Rows : ", row(i))

}
for(i<- 0 to row.length){
	println("col : ", col(i))

}
for(i<- 0 to row.length){
	println("val : ", values(i))

}
// println("Cols : ", col)
// println("values : ", values)

val matrix = Matrices.sparse(5, 5, col, row, values)

println(matrix)

// val allLines = Source.fromFile("small-data").getLines.map{ line=> 
// 			line.split(" ")
// }

// val first = allLines(0)




// val numFeatures = data.take(1)(0).features.size

// val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)

// // Append 1 into the training data as intercept.
// val training = splits(0).map(x => (x.label, MLUtils.appendBias(x.features))).cache()



// var n=3
// val sampledData = data.takeSample(false,n)
// println
// println
// val training = sampledData.map(x => (x.label, MLUtils.appendBias(x.features))).cache()
// val training = sampledData.map(x => x.multiply(2))

// for ( i <- 0 to n-1 )
// {
// 	val new1 = allValues(i)
// 	println(i) 
// 	println(new1.label)
// 	println(new1.features.size)
// 	println
// }


// val a1 = sampledData.take(1)
// val a2 = sampledData.take(2)

// var i = 0
// var allValues = sampledData.take(n)





// val arrayOfVector = sampledData.map(lp => lp.features)
// println
// println

// // val matrix = Matrices.dense(arrayOfVector(0))

// val this1 = arrayOfVector(0)
// println
// println

// val sparseVector = arrayOfVector.take(1)(0)
// println
// println

// // Datatype : breeze.linalg.support.TensorValues[Int,Double,breeze.linalg.Vector[Double]] 
// // print (sparseVector.values) : gives TensorValues only

// val valueMatrix = sparseVector.values
// println
// println

// //breeze.linalg.support.TensorKeys[Int,Double,breeze.linalg.Vector[Double]] 
// val indexMatrix = sparseVector.keys
// println
// println

// val indexMatrix2 = sparseVector.keySet
// println
