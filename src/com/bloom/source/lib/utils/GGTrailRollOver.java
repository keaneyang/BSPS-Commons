package com.bloom.source.lib.utils;

import java.io.File;

public class GGTrailRollOver
  extends DefaultFileComparator
{
  public int compare(Object o1, Object o2)
  {
    File f1 = (File)o1;
    File f2 = (File)o2;
    
    long diff = extractSeqNumber(f1.getName()) - extractSeqNumber(f2.getName());
    if (diff == 0L) {
      diff = getFileCreationTime(f1) - getFileCreationTime(f2);
    }
    return (int)diff;
  }
  
  private int extractSeqNumber(String name)
  {
    int len = name.length();
    while ((len > 0) && 
      (Character.isDigit(name.charAt(len - 1)) == true)) {
      len--;
    }
    String sequence = name.substring(len);
    return Integer.parseInt(sequence);
  }
}
