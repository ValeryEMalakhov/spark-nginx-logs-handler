package com.prb.dnhs.fs

import org.apache.hadoop.fs.{FileSystem, Path}

abstract class FileSystemEnvCleaner {

  val fs: FileSystem

  val hdfsPath: String

  def cleanup(pathToFiles: Path): Unit = {
    val processedPath = new Path(s"$hdfsPath/processed")

    fs.moveFromLocalFile(pathToFiles, processedPath)
  }
}
