package utils

import java.io.{IOException, _}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

import scala.collection.mutable.ListBuffer


/**
  * Author：wangpeizhe
  * Date：2023年08月14日 14:47
  * Desc：对于org.apache.hadoop.fs.path来说
  *  path.getName只是文件名，不包括路径
  *  path.getParent也只是父文件的文件名，同样不包括路径
  *  path.toString才是文件的全部路径
  */

object HdfsUtils {

  def getFS(): FileSystem = {
    val conf = new Configuration() //load core-default.xml and core-site.xml
    Configuration.addDefaultResource("hdfs-site.xml") //CLASSPATH资源
    FileSystem.get(conf)
  }

  def colseFS(fs: FileSystem): Unit = {
    if (fs != null) {
      try {
        fs.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }

  def isDirectory(fs: FileSystem, fullName: String): Boolean = {
    isDirectory(fs, new Path(fullName))
  }

  def isDirectory(fs: FileSystem, path: Path): Boolean = {
    fs.isDirectory(path)
  }

  def isFile(fs: FileSystem, fullName: String): Boolean = {
    isFile(fs, new Path(fullName))
  }

  def isFile(fs: FileSystem, path: Path): Boolean = {
    fs.isFile(path)
  }

  def createNewFile(fs: FileSystem, fullName: String): Boolean = {
    createNewFile(fs, new Path(fullName))
  }

  /**
    * hdfs文件系统在创建filename对应的文件时，如果相关的文件夹不存在，会自动创建相关的文件夹
    *
    * @param fs
    * @param path
    * @return
    */
  def createNewFile(fs: FileSystem, path: Path): Boolean = {
    fs.createNewFile(path)
  }

  def mkdirs(fs: FileSystem, fullName: String): Boolean = {
    mkdirs(fs, new Path(fullName))
  }

  def mkdirs(fs: FileSystem, path: Path): Boolean = {
    fs.mkdirs(path)
  }

  def exists(fs: FileSystem, fullName: String): Boolean = {
    exists(fs, new Path(fullName))
  }

  def exists(fs: FileSystem, path: Path): Boolean = {
    fs.exists(path)
  }

  def createLocalFile(fullName: String): File = {
    val target: File = new File(fullName)
    if (!target.exists()) {
      val index: Int = fullName.lastIndexOf(File.separator)
      val parentFullName: String = fullName.substring(0, index)
      val parent: File = new File(parentFullName)
      if (!parent.exists) {
        parent.mkdirs()
      } else if (!parent.isDirectory) {
        parent.mkdir
      }
      target.createNewFile()
    }
    target
  }

  def listFiles(fs: FileSystem, path: Path, prefix: String, suffix: String, holder: ListBuffer[Path]): ListBuffer[Path] = {
    //    读取hdfs目录:hadoop dfs -ls path相当于listStatus的简写
    val filesStatus = fs.listStatus(path, new PathFilter {
      override def accept(path: Path): Boolean = {
        var flag: Boolean = true
        if (isFile(fs, path)) {
          val name = path.getName
          if (prefix != null && prefix.trim.length > 0) {
            flag = name.startsWith(prefix)
          }
          if (flag && prefix != null && prefix.trim.length > 0) {
            flag = name.endsWith(suffix)
          }
        }
        flag
      }
    })
    for (status <- filesStatus){
      val filePath: Path = status.getPath
      if (isFile(fs,filePath))
        holder += filePath
      else
        listFiles(fs,filePath,prefix,suffix,holder)
    }
    holder
  }

  def getMergeWithHeader(fs: FileSystem, srcDir: Path, target: String, deleteSource: Boolean, header: String, suffix: String): Unit = {
    val tabName: String = srcDir.getName
    val localFile: File = createLocalFile(target + File.separator + tabName + suffix)
    val outputStream: FileOutputStream = new FileOutputStream(localFile)
    try {
      if (header != null && header.trim.length > 0) {
        outputStream.write((header + "\n").getBytes("UTF-8"))
      }
      try {
        val holder: ListBuffer[Path] = new ListBuffer[Path]
        val children: List[Path] = listFiles(fs, srcDir, "part", suffix, holder).toList
        for (child <- children) {
          val inputStream: FSDataInputStream = fs.open(child)
          try {
            IOUtils.copyBytes(inputStream, outputStream, fs.getConf, false)
          } finally {
            inputStream.close()
          }
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
    } finally {
      outputStream.close()
    }
    println(s"Get Merge ${tabName} to ${localFile.getAbsolutePath}")

    if (deleteSource) {
      fs.delete(srcDir, true)
    }
  }

}

