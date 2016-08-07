package com.bloom.source.lib.intf;

import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.uuid.UUID;
import com.bloom.common.exc.AdapterException;
import com.bloom.event.Event;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

public class BaseParser
  implements Parser, Iterator<Event>
{
  Map<String, Object> prop;
  UUID srcId;
  
  public BaseParser(Map<String, Object> property, UUID uuid)
    throws AdapterException
  {
    this.prop = property;
    this.srcId = uuid;
  }
  
  public Iterator<Event> parse(InputStream in)
    throws Exception
  {
    Reader reader;
    if (!(in instanceof Reader))
    {
      this.prop.put(Reader.STREAM, in);
      this.prop.put(Reader.READER_TYPE, Reader.STREAM_READER);
      reader = Reader.createInstance(new Property(this.prop));
    }
    else
    {
      reader = (Reader)in;
    }
    return parse(reader);
  }
  
  public Iterator<Event> parse(Reader reader)
    throws Exception
  {
    return null;
  }
  
  public void close()
    throws Exception
  {}
  
  public boolean hasNext()
  {
    return false;
  }
  
  public Event next()
  {
    return null;
  }
  
  public void remove() {}
}
