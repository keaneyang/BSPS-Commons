package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class CommentEvent
  extends SMEvent
{
  public CommentEvent()
  {
    super((short)6);
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
