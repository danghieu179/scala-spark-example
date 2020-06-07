import java.nio.file.{Paths, Files}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io._
import spark.implicits._

object ReadParquetFile {
  def main(args : Array[String]) : Unit = {
    if (args.length == 0) {
        return println("dude, i need at least one parameter")
    }
    var pathFile = ""
    try
    { 
      // Read file from input
      pathFile = args(0)
      val parquetFileDF =  spark.read.parquet(pathFile)
      //Parquet files can also be registered as tables and then used in SQL statements.
      parquetFileDF.createOrReplaceTempView("parquetFile")
    }
    catch
    {  
      case ex: FileNotFoundException => {
        return println("File not found")
      }
      case error: Exception => {
        return println(s"Error: $error")
      }
    }
    // Convert the path string to a Path object and get the "base name" from that path.
    val fileName = Paths.get(pathFile).getFileName            
    val folderName = fileName.toString.split("\\.")(0)
    // Create new folder for contain report files
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val folderPath = currentDirectory+"/"+folderName
    new java.io.File(folderPath).mkdirs
    // run function compute
    totalUser(folderPath)
    sumGender(folderPath)
    sumAge(folderPath)
    spark.catalog.dropTempView("parquetFile")
  }
  
  def sumGender(newFolder: String): Unit = {
    val userByGender = spark.sql("""
      SELECT COALESCE(NULLIF(gender,''),'Unknow') as gender, count(*) as reg_count FROM parquetFile GROUP BY gender ORDER BY gender DESC
    """)
    //create header for csv
    val headerDF = Seq(("gender", "reg_count")).toDF("gender", "reg_count")
    // delete file if existed
    new File(newFolder+"/"+"merged_total_user_by_gender.csv").delete()
    var fileName = "total_user_by_gender.csv"
    var outputFileName = newFolder + "/temp_" + fileName 
    var mergedFileName = newFolder + "/merged_" + fileName
    var mergeFindGlob  = outputFileName
    // add header and create file csv
    headerDF.union(userByGender).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "false").save(outputFileName)
    // merge file csv
    merge(mergeFindGlob, mergedFileName )
    userByGender.unpersist()
  }

  def sumAge(newFolder: String): Unit = {
    val userByAge = spark.sql(
      """
      SELECT age_range, reg_count
      FROM 
        (
          SELECT '<16' as age_range , count(*) as reg_count, 0 as seq
          FROM parquetFile 
          WHERE ROUND(DATEDIFF(current_date(), to_date(birthdate, "MM/dd/yyyy")) / 365) < 16

          UNION

          SELECT '16-34' as age_range , count(*) as reg_count, 1 as seq
          FROM parquetFile 
          WHERE ROUND(DATEDIFF(current_date(), to_date(birthdate, "MM/dd/yyyy")) / 365) BETWEEN 16 AND 34

          UNION

          SELECT '35-59' as age_range , count(*) as reg_count, 2 as seq 
          FROM parquetFile 
          WHERE ROUND(DATEDIFF(current_date(), to_date(birthdate, "MM/dd/yyyy")) / 365) BETWEEN 35 AND 59

          UNION

          SELECT '>=60' as age_range , count(*) as reg_count, 3 as seq 
          FROM parquetFile 
          WHERE ROUND(DATEDIFF(current_date(), to_date(birthdate, "MM/dd/yyyy")) / 365) >=60

          UNION

          SELECT 'Unknow' as age_range , count(*) as reg_count, 4 as seq 
          FROM parquetFile 
          WHERE ROUND(DATEDIFF(current_date(), to_date(birthdate, "MM/dd/yyyy")) / 365, 0) is Null
        )
        ORDER BY seq
      """
    )
    //create header for csv
    val headerDF = Seq(("age_range", "reg_count")).toDF("age_range", "reg_count")
    // delete file if existed
    new File(newFolder+"/"+"merged_total_user_by_age.csv").delete()
    var fileName = "total_user_by_age.csv"
    var outputFileName = newFolder + "/temp_" + fileName 
    var mergedFileName = newFolder + "/merged_" + fileName
    var mergeFindGlob  = outputFileName
    // add header and create file csv
    headerDF.union(userByAge).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "false").save(outputFileName)
    // merge file csv
    merge(mergeFindGlob, mergedFileName )
    userByAge.unpersist()
  }

  def totalUser(newFolder: String): Unit = {
    val totalUser = spark.sql("SELECT count(registration_dttm) as total_user FROM parquetFile").first()
    val result =  "Total user registered: " + totalUser.get(0)
    // create text file and write result to it
    val pw = new PrintWriter(new File(newFolder+"/"+"total_user.txt"))
    pw.write(result)
    pw.close
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null) 
    // the "true" setting deletes the source files once they are merged into the new output
  }
}

