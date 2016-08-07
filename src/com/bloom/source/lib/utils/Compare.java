package com.bloom.source.lib.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import org.apache.log4j.Logger;

public class Compare
  implements Comparable<Object>
{
  public long time;
  public File file;
  private Logger logger = Logger.getLogger(Compare.class);
  
  public Compare(File file)
  {
    this.file = file;
    this.time = getFileCreationTime(file);
  }
  
  private long getFileCreationTime(File file)
  {
    Path path = file.toPath();
    long creationTime = 0L;
    try
    {
      BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, new LinkOption[0]);
      creationTime = attributes.creationTime().toMillis();
    }
    catch (IOException e)
    {
      this.logger.error(e);
    }
    return creationTime;
  }
  
  public int compareTo(Object comp)
  {
    long u = ((Compare)comp).time;
    return this.time == u ? 0 : this.time < u ? -1 : 1;
  }
}
