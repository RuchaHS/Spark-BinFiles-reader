import org.apache.spark.sql.SparkSession

object ReadBinFiles {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("ReadBin")
      .getOrCreate()

    val readData = spark.read
      .format("cobol")
      .option("copybook","file:////Users/r0s0buo/Documents/CILL-wolves/copybooki0481.txt")
      .option("encoding","EBCDIC")
      .load("file:////Users/r0s0buo/Documents/temp/io481bin.bin")

    readData.printSchema()
    readData.show(false)

    readData.write.json("file:////Users/r0s0buo/Desktop/dec13-longtext")
  }
}
