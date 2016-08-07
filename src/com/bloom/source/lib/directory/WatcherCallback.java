package com.bloom.source.lib.directory;

import java.io.File;

public abstract interface WatcherCallback
{
  public abstract void onFileCreate(File paramFile);
  
  public abstract void onFileDelete(File paramFile);
  
  public abstract void onFileModify(File paramFile);
  
  public abstract void onFileDelete(String paramString1, String paramString2);
}

