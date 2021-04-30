package svd

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.sql.SparkSession

object SVDTest extends App {

  val spark = SparkSession.builder().
    master("local[*]").
    appName("SVD-Test").
    getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val data = Array(
    Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
    Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
    Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
  println(data.getClass.getName)

  val rows = spark.sparkContext.parallelize(data)
  println(rows.getClass.getName)

  val mat = new RowMatrix(rows)
  println(mat.getClass.getName)

  // Compute the top 5 singular values and corresponding singular vectors.
  val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(5, computeU = true)
  val U: RowMatrix = svd.U // The U factor is a RowMatrix.
  val s: linalg.Vector = svd.s // The singular values are stored in a local dense vector.
  val V: Matrix = svd.V // The V factor is a local dense matrix.

  val collect = U.rows.collect()
  println("U factor is:")
  collect.foreach { vector => println(vector) }
  println(s"Singular values are: $s")
  println(s"V factor is:\n$V")

  spark.stop()
}
