package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class RowEndEvent
  extends SMEvent
{
  public RowEndEvent()
  {
    super((short)11);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
