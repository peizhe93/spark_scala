package etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Author：wangpeizhe
  * Date：2023年08月14日 9:38
  * Desc：
  */

object DataExportEtl {

  //  @Transient的作用是指定该属性或字段不是永久的(会序列化，但不会持久化)。 它用于注释实体类，映射超类或可嵌入类的属性或字段
  @transient lazy private val logger: Logger = LoggerFactory.getLogger(DataExportEtl.getClass.getName)

  def main(args: Array[String]): Unit = {
    val Array(hqlStr, database, tblName) = args

    val spark = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", "255")
      // Orc的分split有3种策略(ETL、BI、HYBIRD)，默认是HYBIRD(混合模式，根据文件大小和文件个数自动选择ETL还是BI模式)，BI策略以文件为粒度进行split划分；
      // ETL策略会将文件进行切分，多个stripe组成一个split；
      // HYBRID策略为：当文件的平均大小大于hadoop最大split值（默认256 * 1024 * 1024）时使用ETL策略，否则使用BI策略。
      // 对于一些较大的ORC表，可能其footer较大，ETL策略可能会导致其从hdfs拉取大量的数据来切分split，甚至会导致driver端OOM，因此这类表的读取建议使用BI策略。
      // 对于一些较小的尤其有数据倾斜的表（这里的数据倾斜指大量stripe存储于少数文件中），建议使用ETL策略
      .config("hive.exec.orc.split.strategy", "ETL")
      .enableHiveSupport()
      .getOrCreate()

    logger.info("hqlStr:" + hqlStr)

    logger.error("hqlStr:" + hqlStr)

    val hqlStrRep: String = hqlStr.replaceAll("\"<FLAG>\"", " ")

    logger.info("hqlStrRep:" + hqlStrRep)

    logger.error("hqlStrRep:" + hqlStrRep)

    spark.udf.register("nulls", () => {
      null.asInstanceOf[String]
    })

    spark.sql(s"drop table if exists ${database}.${tblName}")
    spark.sql(s"create table ${database}.${tblName} ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\012' STORED AS TEXTFILE as ${hqlStrRep}")
    val sql_result: DataFrame = spark.sql(hqlStrRep)
    sql_result.registerTempTable(s"${tblName}_tmp")
    spark.sql(s"insert overwrite table ${database}.${tblName} select * from ${tblName}_tmp")

    logger.info("插入语句已经执行")

  }
}
