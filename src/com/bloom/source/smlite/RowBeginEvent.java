package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class RowBeginEvent
  extends SMEvent
{
  public RowBeginEvent()
  {
    super((short)8);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
