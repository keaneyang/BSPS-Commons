package com.bloom.source.lib.reader;

import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import java.nio.CharBuffer;

public class StringReader
  extends ReaderBase
{
  private CharBuffer buffer;
  
  protected StringReader(Property prop)
    throws AdapterException
  {
    super(prop);
  }
  
  public Object readBlock()
    throws AdapterException
  {
    return this.buffer;
  }
  
  public void setCharacterBuffer(CharBuffer buffer)
  {
    this.buffer = buffer;
  }
}
