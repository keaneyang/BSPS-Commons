package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class QuoteBeginEvent
  extends SMEvent
{
  public QuoteBeginEvent()
  {
    super((short)4);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
