package com.bloom.source.sm;

import com.bloom.source.lib.constant.Constant;

public class DelimiterProcessor
{
  private Constant.recordstatus event;
  
  public DelimiterProcessor(String[] _token, Constant.recordstatus _event)
  {
    this.event = _event;
  }
  
  public Constant.recordstatus process()
  {
    return this.event;
  }
}
