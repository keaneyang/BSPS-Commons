package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class ColumnEvent
  extends SMEvent
{
  public ColumnEvent()
  {
    super((short)1);
    this.removePattern = true;
  }
  
  public ColumnEvent(short st)
  {
    super(st);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
