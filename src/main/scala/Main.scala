import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {

  private def coPurchaseAnalysisSpark(inputPath: String, outputPath: String): Unit = {
    val spark = SparkSession.builder()
      .appName("Co-Purchase Analysis")
      .master("local[*]")
      .config("spark.eventLog.enabled", "false")
      .getOrCreate()

    val cores = spark.conf.get("spark.executor.cores", "4").toInt
    val nodes = spark.conf.get("spark.executor.instances", "4").toInt
    val partitions = math.max(cores * nodes * 2, spark.sparkContext.defaultParallelism * 2)

    //println(s"Cores per executor: $cores")
    //println(s"Numero di executor: $nodes")

    val startTime = System.nanoTime()

    val data: RDD[(Int, Int)] = spark.sparkContext
      .textFile(inputPath)
      .map(line => {
        val parts = line.split(",")
        (parts(0).toInt, parts(1).toInt)
      })

    val hashPartitionedData: RDD[(Int, Int)] = data.partitionBy(new HashPartitioner(partitions))

    val groupedByOrder: RDD[(Int, Iterable[Int])] = hashPartitionedData.groupByKey()

    val productPairs: RDD[((Int, Int), Int)] = groupedByOrder.flatMap {
      case (orderId, products) =>
        val productSet = products.toSet
        for {
          p1 <- productSet
          p2 <- productSet
          if p1 < p2
        } yield ((p1, p2), 1)
    }

    val coPurchaseCounts: RDD[((Int, Int), Int)] = productPairs.reduceByKey(_ + _)

    coPurchaseCounts
      .map { case ((p1, p2), count) => s"$p1,$p2,$count" }
      .coalesce(1)
      .saveAsTextFile(outputPath)

    val endTime = System.nanoTime()
    val totalTime = (endTime - startTime) / 1e9

    println(f"Analisi completata in $totalTime%.2f secondi")

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    val inputPath = if (args.nonEmpty) args(0) else "gs://copurchase-analysis-2025/orders.csv"
    val outputPath = if (args.length >= 2) args(1) else "gs://copurchase-analysis-2025/output"

    if (args.length < 2) {
      println(s"Uso: Main <input_file> <output_directory>")
      println(s"Usando valori di default: input='$inputPath', output='$outputPath'")
    }

    coPurchaseAnalysisSpark(inputPath, outputPath)
  }
}