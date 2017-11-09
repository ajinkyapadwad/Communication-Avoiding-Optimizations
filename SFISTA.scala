import org.apache.spark.mllib.util.MLUtils
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector, DenseVector}

def toBreeze(v: Vector): BV[Double] = v match {
  case DenseVector(values) => new BDV[Double](values)
  case SparseVector(size, indices, values) => {
    new BSV[Double](indices, values, size)
  }
}

val data = MLUtils.loadLibSVMFile(sc, "sample_libsvm_data.txt")
// val numFeatures = data.take(1)(0).features.size

val sampledData = data.takeSample(false, 3)
println

val arrayOfVector = sampledData.map(lp => (toBreeze(lp.features)))
println

val sparseVector = arrayOfVector.take(1)(0)
println
println


// Datatype : breeze.linalg.support.TensorValues[Int,Double,breeze.linalg.Vector[Double]] 
// print (sparseVector.values) : gives TensorValues only

val valueMatrix = sparseVector.values
println
println

//breeze.linalg.support.TensorKeys[Int,Double,breeze.linalg.Vector[Double]] 
val indexMatrix = sparseVector.keys