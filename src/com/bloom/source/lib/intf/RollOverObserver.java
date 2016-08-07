package com.bloom.source.lib.intf;

import java.io.IOException;

public abstract interface RollOverObserver
{
  public abstract void preRollover()
    throws IOException;
  
  public abstract void postRollover(String paramString)
    throws IOException;
}

