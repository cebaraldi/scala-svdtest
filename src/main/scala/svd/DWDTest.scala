package svd

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDate
import scala.collection.JavaConverters._

object DWDTest extends App {

  val spark = SparkSession.builder().
    master("local[*]").
    appName("DWD SVD Test").
    getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val currentDate = LocalDate.now()
  val startDate = LocalDate.of(1991, 1, 1)
  val endDate = LocalDate.of(2020, 12, 31)

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
    .collectAsList.asScala.map(_.doubleValue).toList
  //  val l0 = h.filter('sid === 1420).select('tavg).map(f => f.getDouble(0))
  //    .collectAsList.asScala.map(_.doubleValue)(breakOut)
  val l1 = h.filter('sid === 7341).select('tavg).map(f => f.getDouble(0))
    .collectAsList.asScala.map(_.doubleValue).toList
  val l2 = h.filter('sid === 917).select('tavg).map(f => f.getDouble(0))
    .collectAsList.asScala.map(_.doubleValue).toList

  val data = Array(
    Vectors.dense(l0.toArray),
    Vectors.dense(l1.toArray),
    Vectors.dense(l2.toArray)
  )

  val rows: RDD[Vector] = spark.sparkContext.parallelize(data) // an RDD of local vectors
  val nosv = rows.count().toInt // unless matrix will be transposed

  // Create a RowMatrix from an RDD[Vector].
  val mat = new RowMatrix(rows, nosv, 366)

  //  // Get its size.
  //  println(s"rows = ${mat.numRows()}")
  //  println(s"cols = ${mat.numCols()}")


  //  val qrResult = mat.tallSkinnyQR(true)
  //  qrResult.R
  //  21/04/30 19:27:29 WARN LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeSystemLAPACK
  //  21/04/30 19:27:29 WARN LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeRefLAPACK

  // Compute the top 'nosv' singular values and corresponding singular vectors.
  val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(nosv, computeU = true)
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
