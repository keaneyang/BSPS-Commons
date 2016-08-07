package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class TimeStampEvent
  extends SMEvent
{
  public TimeStampEvent()
  {
    super((short)9);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
