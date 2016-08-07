package com.bloom.source.nvp.sm;

import com.bloom.source.lib.constant.Constant.status;
import com.bloom.source.lib.intf.State;

public abstract class NVPState
  implements State
{
  public abstract status canAccept(char paramChar);
}
