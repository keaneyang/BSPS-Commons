package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class ResetEvent
  extends SMEvent
{
  public char[] buffer;
  
  public ResetEvent()
  {
    super((short)0);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
