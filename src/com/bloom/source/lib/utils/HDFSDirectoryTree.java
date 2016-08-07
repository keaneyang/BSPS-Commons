package com.bloom.source.lib.utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSDirectoryTree
  extends DirectoryTree
{
  private FileSystem hadoopFileSystem;
  
  public HDFSDirectoryTree(String rootDirectory, Field field, Map<String, Object> properties, FileSystem hadoopFileSystem)
  {
    super(rootDirectory, field, properties);
    this.hadoopFileSystem = hadoopFileSystem;
    this.separator = "/";
  }
  
  protected boolean createDirectory(String directoryName)
    throws IOException
  {
    Path directory = new Path(directoryName);
    this.directoryCache.put(directoryName, directory);
    if (this.hadoopFileSystem.mkdirs(directory)) {
      return true;
    }
    if (this.hadoopFileSystem.exists(directory)) {
      return true;
    }
    throw new IOException("Failure in creating directory " + directory.getName() + " in HDFS");
  }
}
