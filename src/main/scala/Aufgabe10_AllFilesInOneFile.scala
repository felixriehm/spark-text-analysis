import java.io.{File, FileWriter}
import java.time.Duration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
import scala.reflect.io.Directory

object Aufgabe10_AllFilesInOneFile {
  //Constants
  val AppName: String = "aufgabe10"
  val Languages = List("Dutch", "English", "French", "German", "Italian", "Russian", "Spanish", "Ukrainian")

  //File paths
  val ResourcesDir = "src/main/resources/"
  val AnalysisDir: String = ResourcesDir + "analysis/"
  val ResultDir: String = ResourcesDir + "result/allFilesInOne/"
  val StopWordsDir: String = ResourcesDir + "stopwords/"
  var start = 0L
  var end = 0L

  def main(args: Array[String]) {
    start = System.nanoTime
    //Init spark
    val conf = new SparkConf().setAppName(AppName).setMaster("local[*]").set("spark.driver.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    //Clear result folders
    val resultFolder = new Directory(new File(ResultDir))
    resultFolder.deleteRecursively()

    //Start counting words and filtering them
    createFileContainingAllTexts()
    val allWords = countWords(sc)
    filterStopWords(sc, allWords);

    //calculate completion time
    end = System.nanoTime
    val duration = Duration.ofNanos(end - start)
    printf("%d Hours %d Minutes %d Seconds%n",
      duration.toHours(), duration.toMinutes() % 60, duration.getSeconds() % 60);
  }

  /**
   * Gets list of files from a specific directory
   *
   * @param dir language directory
   * @return list of all files in that directory
   */
  def getListOfFiles(dir: String): List[File] = {
    //Get files of a directory
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  /**
   * Creates a file containing the texts of all files in AnalysisDir
   */
  def createFileContainingAllTexts() = {
    new File(AnalysisDir + "allFilesCombined.txt").delete() //deletes the old file
    val fw = new FileWriter(AnalysisDir + "allFilesCombined.txt", true)
    for (language <- Languages) {
      val fileList = getListOfFiles(AnalysisDir + language)
      for (file <- fileList) {
        fw.write(Source.fromFile(file).mkString) //adds file content to file containing all texts
      }
    }
  }

  /**
   * Filters the words from the file containing all files
   *
   * @param sc spark context
   */
  def countWords(sc: SparkContext): RDD[(String, Int)] = {
    val textFile = sc.textFile(AnalysisDir + "allFilesCombined.txt")
    //Filter words
    val counts = textFile.flatMap(line => line.split("\\PL+"))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)
      .subtractByKey(sc.makeRDD(Array(("", 1)))) //remove flatMap => split entry of empty lines
      .sortBy(_._2, ascending = false)
    //Save output
    counts.map(entry => s"${entry._1} : ${entry._2}").coalesce(1)
      .saveAsTextFile(ResultDir + "/withoutStopwordsRemoved")
    counts
  }

  /**
   * Filters out stopwords from countWords() result
   *
   * @param sc       spark context
   * @param allWords allWords from countWords() method
   */
  def filterStopWords(sc: SparkContext, allWords: RDD[(String, Int)]) = {
    var stopwords = sc.makeRDD(Array(("", 1)))
    for (language <- Languages) {
      stopwords = stopwords ++ sc.textFile(StopWordsDir + language + ".txt").map(word => (word.toLowerCase, 1)).reduceByKey(_ + _)
    }
    //Remove stopwords from all words
    allWords.subtractByKey(stopwords).reduceByKey(_ + _).sortBy(_._2, ascending = false)
      .zipWithIndex().filter(_._2 < 10).coalesce(1)
      .map(entry => s"#${entry._2}: ${entry._1._1} (${entry._1._2})").saveAsTextFile(ResultDir + "/withStopwordsRemoved")
  }
}