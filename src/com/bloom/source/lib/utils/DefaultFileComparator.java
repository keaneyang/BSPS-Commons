package com.bloom.source.lib.utils;

import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

public class DefaultFileComparator
  implements Comparator<Object>
{
  private Logger logger = Logger.getLogger(DefaultFileComparator.class);
  FileSystem hadoopFileSystem;
  
  public DefaultFileComparator() {}
  
  public DefaultFileComparator(FileSystem hadoopFileSystem)
  {
    this.hadoopFileSystem = hadoopFileSystem;
  }
  
  public int compare(Object file1, Object file2)
  {
    Object f1 = file1;
    Object f2 = file2;
    int diff = (int)(getFileCreationTime(f1) - getFileCreationTime(f2));
    return diff;
  }
  
  public long getFileCreationTime(Object file)
  {
    long creationTime = 0L;
    if ((file instanceof File))
    {
      java.nio.file.Path path = ((File)file).toPath();
      try
      {
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, new LinkOption[0]);
        creationTime = attributes.creationTime().toMillis();
      }
      catch (IOException e)
      {
        if (this.logger.isDebugEnabled()) {
          this.logger.debug("Couldn't compare file creation time");
        }
      }
    }
    else if ((file instanceof org.apache.hadoop.fs.Path))
    {
      org.apache.hadoop.fs.Path path = (org.apache.hadoop.fs.Path)file;
      try
      {
        FileStatus fs = this.hadoopFileSystem.getFileStatus(path);
        creationTime = fs.getModificationTime();
      }
      catch (IOException e)
      {
        if (this.logger.isDebugEnabled()) {
          this.logger.debug("Couldn't compare file creation time");
        }
      }
    }
    else if (this.logger.isDebugEnabled())
    {
      this.logger.debug("Unsupported object. Not going to compare file creation time");
    }
    return creationTime;
  }
  
  public String getFileName(Object file)
    throws AdapterException
  {
    String fileName;
    if ((file instanceof File))
    {
      fileName = ((File)file).getName();
    }
    else
    {
      if ((file instanceof org.apache.hadoop.fs.Path))
      {
        fileName = ((org.apache.hadoop.fs.Path)file).getName();
      }
      else
      {
        AdapterException se = new AdapterException("Unsupported file instance");
        throw se;
      }
    }
    return fileName;
  }
}
