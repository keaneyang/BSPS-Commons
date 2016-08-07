package com.bloom.source.lib.utils;

import com.bloom.common.exc.AdapterException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

public class NameAndCreationTime
  extends DefaultFileComparator
{
  private Logger logger = Logger.getLogger(NameAndCreationTime.class);
  
  public NameAndCreationTime() {}
  
  public NameAndCreationTime(FileSystem hadoopFileSystem)
  {
    super(hadoopFileSystem);
  }
  
  public int compare(Object o1, Object o2)
  {
    Object file1 = o1;
    Object file2 = o2;
    long diff = 0L;
    try
    {
      diff = getFileName(file1).compareTo(getFileName(file2));
      if (diff == 0L) {
        diff = getFileCreationTime(file1) - getFileCreationTime(file2);
      }
    }
    catch (AdapterException e)
    {
      this.logger.error(e.getErrorMessage());
    }
    return (int)diff;
  }
}
