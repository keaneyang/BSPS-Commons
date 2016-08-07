package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class EscapeEvent
  extends SMEvent
{
  public EscapeEvent()
  {
    super((short)10);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
