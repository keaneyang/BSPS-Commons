package com.bloom.source.lib.rollingpolicy.util;

public class DynamicRollOverFilenameFormat
  extends RolloverFilenameFormat
{
  private String currentWorkingDirectoryName;
  
  public DynamicRollOverFilenameFormat(String pattern, long fileLimit, int startSequence, int incrementSequenceBy, boolean addDefaultSequence, String currentWorkingDirectoryName)
  {
    super(pattern, fileLimit, startSequence, incrementSequenceBy, addDefaultSequence);
    this.currentWorkingDirectoryName = currentWorkingDirectoryName;
  }
  
  public String getNextSequence()
  {
    return this.currentWorkingDirectoryName + super.getNextSequence();
  }
}
