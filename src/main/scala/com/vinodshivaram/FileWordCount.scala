import com.vinodshivaram.StreamingLogger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: NetworkWordCount <filepath>")
      System.exit(1)
    }

    StreamingLogger.setStreamingLogLevels()

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreach(wordCount => println(wordCount))
  }
}