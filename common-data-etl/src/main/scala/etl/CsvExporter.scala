package etl

import org.apache.hadoop.fs._
import utils.HdfsUtils

/**
  * Author：wangpeizhe
  * Date：2023年08月14日 14:43
  * Desc：
  */

object CsvExporter {

  def main(args: Array[String]): Unit = {
    val Array(hdfsDir, localDir, suffix, header, deleteHdfs) = args
    var deleteSource = false
    if (deleteHdfs != null && "true".equals(deleteHdfs)) {
      deleteSource = true
    }

    var headerStr = header
    if (header == null || "NULL".equals(header)) {
      headerStr = ""
    }

    val fs: FileSystem = HdfsUtils.getFS()
    HdfsUtils.getMergeWithHeader(fs, new Path(hdfsDir), localDir, deleteSource, headerStr, suffix)
    HdfsUtils.colseFS(fs)

  }
}
