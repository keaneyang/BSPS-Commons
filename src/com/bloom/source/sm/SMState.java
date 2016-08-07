package com.bloom.source.sm;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.intf.State;

public abstract class SMState
  implements State
{
  public abstract Constant.status canAccept(char paramChar);
  
  public abstract void reset();
  
  public abstract String getCharsOfInterest();
}

