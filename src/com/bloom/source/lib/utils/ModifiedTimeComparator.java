package com.bloom.source.lib.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

public class ModifiedTimeComparator
  extends DefaultFileComparator
{
  private Logger logger = Logger.getLogger(DefaultFileComparator.class);
  
  public long getFileCreationTime(Object file)
  {
    long creationTime = 0L;
    if ((file instanceof File))
    {
      java.nio.file.Path path = ((File)file).toPath();
      try
      {
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, new LinkOption[0]);
        creationTime = attributes.lastModifiedTime().toMillis();
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
}
