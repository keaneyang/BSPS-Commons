package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class RowEvent
  extends SMEvent
{
  char[] buffer;
  int rowBegin;
  
  public RowEvent(char[] array)
  {
    super((short)2);
    this.buffer = array;
  }
  
  public RowEvent()
  {
    super((short)2);
    this.removePattern = true;
  }
  
  public void rowBegin(int offset)
  {
    this.rowBegin = offset;
  }
  
  public int rowBegin()
  {
    return this.rowBegin;
  }
  
  public char[] array()
  {
    return this.buffer;
  }
  
  public void array(char[] a)
  {
    this.buffer = a;
  }
  
  public void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
}
