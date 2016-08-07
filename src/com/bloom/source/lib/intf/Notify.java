package com.bloom.source.lib.intf;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.constant.Constant.eventType;

import java.io.File;
import java.io.IOException;

public abstract interface Notify
{
  public abstract void handleEvent(Constant.eventType parameventType, File paramFile)
    throws IOException;
}

