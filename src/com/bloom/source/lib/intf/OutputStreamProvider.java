package com.bloom.source.lib.intf;

import java.io.IOException;
import java.io.OutputStream;

public abstract interface OutputStreamProvider
{
  public abstract OutputStream provideOutputStream(OutputStream paramOutputStream)
    throws IOException;
}

