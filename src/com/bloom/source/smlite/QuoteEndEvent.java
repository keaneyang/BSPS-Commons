package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class QuoteEndEvent
  extends SMEvent
{
  public QuoteEndEvent()
  {
    super((short)5);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
