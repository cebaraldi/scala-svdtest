package svd

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDate

object DWDTest extends App {

  val spark = SparkSession.builder().
    master("local[*]").
    appName("DWD SVD Test").
    getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val currentDate = LocalDate.now()
  val startDate = LocalDate.of(2010, 1, 1)
  val endDate = LocalDate.of(2020, 12, 31)

  val daysCount = 1000

  import spark.implicits._

  val h = spark.read.parquet("/media/datalake/DWDHistoricalData.parquet").
    filter('date >= startDate && 'date <= endDate).
    filter('sid.isInCollection(List(1420, 7341, 917))). // Frankfurt, Offenbach, Darmstadt
    withColumn("month", month('date)).
    withColumn("day", dayofmonth('date)).
    drop("date").
    select('sid, 'tmk, 'month, 'day).
    groupBy('sid, 'month, 'day).
    agg(
      avg('tmk) as "tavg",
      variance('tmk) as "tvar",
      count('tmk) as "tcount"
    ).
    orderBy('sid, 'month, 'day)
  //  h.show

  val l0 = h.filter('sid === 1420).select('tavg).map(f => f.getDouble(0))
    .collectAsList
  val l1 = h.filter('sid === 7341).select('tavg).map(f => f.getDouble(0))
    .collectAsList
  val l2 = h.filter('sid === 917).select('tavg).map(f => f.getDouble(0))
    .collectAsList
  println(l0)
  println(l0.getClass.getName)

  val x = List(12.0, -51.0, 4.0).toArray
  println(x)
  println("> "+x.getClass.getName)
//  val x = l0.toArray
  val y = List(6.0, 167.0, -68.0).toArray
//  val y = l1.toArray
  val z = List(-4.0, 24.0, -41.0).toArray
  //val x = Seq(12.0, -51.0, 4.0)
  //  val x = Array(12.0, -51.0, 4.0)
  val rows: RDD[Vector] = spark.sparkContext.parallelize(Array(
    Vectors.dense(x),
    Vectors.dense(y)
//    Vectors.dense(z),
//    Vectors.dense(12.0, -51.0, 4.0),
//    Vectors.dense(6.0, 167.0, -68.0),
//    Vectors.dense(-4.0, 24.0, -41.0))
  ))// an RDD of local vectors

  // Create a RowMatrix from an RDD[Vector].
  //  val mat: RowMatrix = new RowMatrix(rows)
  val mat = new RowMatrix(rows)

  // Get its size.
  println(s"rows = ${mat.numRows()}")
  println(s"cols = ${mat.numCols()}")
  //      val mat = new RowMatrix(rows, 366, 2)

  //  val qrResult = mat.tallSkinnyQR(true)
  //  qrResult.R
  //  21/04/30 19:27:29 WARN LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeSystemLAPACK
  //  21/04/30 19:27:29 WARN LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeRefLAPACK

  // Compute the top 5 singular values and corresponding singular vectors.
  val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(2, computeU = true)
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
