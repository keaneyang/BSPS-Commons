package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class EndOfBlockEvent
  extends SMEvent
{
  public EndOfBlockEvent()
  {
    super((short)7);
    this.length = 0;
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
