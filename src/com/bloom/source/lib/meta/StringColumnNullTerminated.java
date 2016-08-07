package com.bloom.source.lib.meta;

import java.nio.ByteBuffer;

public class StringColumnNullTerminated
  extends Column
{
  public int getLengthOfString(ByteBuffer buffer, int stringColumnLength)
  {
    int len = 0;
    byte byte0 = buffer.get();
    while (byte0 != 0)
    {
      len++;
      byte0 = buffer.get();
    }
    setSize(len);
    return len;
  }
  
  public Object getValue(byte[] rowdata, int offset, int length)
  {
    byte[] destination = new byte[length];
    System.arraycopy(rowdata, offset, destination, 0, length);
    return new String(destination);
  }
}
