import org.apache.spark._
import org.apache.spark.sql.SparkSession
import  java.io._
object SparkApp1 {
  def main(args: Array[String]): Unit = {
    sessionSpark()


  }

  def sessionSpark() : Unit = {
    System.setProperty("hadoop.home.dir","C:\\Hadoop")
    val ss = SparkSession.builder()
      //Nom du noeud maitre par defaut local[*]
      .master(master = "local[*]")
      // le nom de la session dans le cluster
      .appName(name = "Ma premiere application")
      /*Pour supporter les requete hive
     .enableHiveSupport()
      */
      // Creer le session
      .getOrCreate()
    val rdd1 = ss.sparkContext.parallelize(Seq("Hello", "Distributed", "World"))
    val rdd2 = ss.read
      .format("textFile")
      .textFile("C:\\spark-2.2.0-bin-hadoop2.7\\examples\\src\\main\\resources\\fichier2.txt")
  // rdd2.foreach(l=>println(l))
   //val coumter =  rdd2.flatMap(l=>l.split(" ")).map(word=>(word,1).reduceByKey(_+_)


    val conf = new SparkConf().setAppName("File Count").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
    /*val si = new SparkContext()
    val rdd1 = si.parallelize(Seq("12","15","18","85"))
    val rdd2  = si.parallelize(Seq("Hello", "Distributed", "World"))*/
    val monfichier = sc.textFile("C:\\spark-2.2.0-bin-hadoop2.7\\examples\\src\\main\\resources\\file.txt")
    val countsFlatMap = monfichier.flatMap(line => line.split(" ")).map(word =>(word,1)).reduceByKey(_+_)

    countsFlatMap.foreach(l=>{
      try {
        val fos: FileOutputStream = new FileOutputStream("code.txt",true)
        val sortir  = new PrintWriter(fos)
         sortir.println(l)
        sortir.close()
      }
    })


    /*wp.write("je suis au debut de ma formation")
    wp.close()*/

   countsFlatMap.collect.foreach(l=>print(l))
    //val rdd3 = rdd2.map(l =>l.toUpperCase())
    //rdd2.foreach(line => println(line))
   // countsFlatMap.saveAsTextFile("C:\\file3")

    //rdd1.foreach(l=>println(l))
  }


}
