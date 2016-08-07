package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class QuoteEvent
  extends SMEvent
{
  public QuoteEvent()
  {
    super((short)3);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
