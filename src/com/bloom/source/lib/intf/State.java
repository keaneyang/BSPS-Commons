package com.bloom.source.lib.intf;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.constant.Constant.status;

public abstract interface State
{
  public abstract Constant.status process(char paramChar);
}

