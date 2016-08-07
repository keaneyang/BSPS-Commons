package com.bloom.source.lib.intf;

import com.bloom.event.Event;
import java.io.InputStream;
import java.util.Iterator;

public abstract interface Parser
{
  public abstract Iterator<Event> parse(InputStream paramInputStream)
    throws Exception;
  
  public abstract void close()
    throws Exception;
}

