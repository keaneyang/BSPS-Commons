package com.bloom.source.lib.directory;

import com.bloom.source.lib.prop.Property;

import java.io.IOException;
import java.util.Map;

public class WatcherFactory
{
  public static String WATCHER_INST = "watcherinst";
  
  public static Watcher createWatcher(Map<String, Object> map, WatcherCallback callback)
    throws IOException
  {
    return createWatcher(new Property(map), callback);
  }
  
  public static Watcher createWatcher(Property prop, WatcherCallback callback)
    throws IOException
  {
    Watcher watcher = null;
    
    watcher = (Watcher)prop.getObject(WATCHER_INST, null);
    if (watcher != null) {
      return watcher;
    }
    if (prop.getBoolean(Property.NETWORK_FILE_SYSTEM, false) == true) {
      watcher = new NFSWatcher(prop, callback);
    } else {
      watcher = new LocalDirectoryWatcher(prop, callback);
    }
    return watcher;
  }
}
