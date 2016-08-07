package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class NVPEvent
  extends RowEvent
{
  public NVPEvent(char[] data)
  {
    super(data);
  }
  
  public NVPEvent() {}
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
