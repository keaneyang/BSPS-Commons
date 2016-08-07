package com.bloom.source.lib.reader;

import com.bloom.common.exc.AdapterException;
import java.nio.CharBuffer;

public class CharBufferManager
  extends BufferManager
{
  public CharBufferManager(ReaderBase strategy)
    throws AdapterException
  {
    super(strategy);
  }
  
  public void init()
    throws AdapterException
  {
    super.init();
  }
  
  protected Object allocateInitialBuffer()
  {
    return CharBuffer.allocate(blockSize() * 2).flip();
  }
  
  protected Object appendData(Object obj, Object data)
  {
    CharBuffer buf = (CharBuffer)obj;
    CharBuffer dataBuf = (CharBuffer)data;
    
    int position = buf.position();
    int limit = buf.limit();
    int length = limit - position;
    
    char[] array = buf.array();
    if (position > 0)
    {
      System.arraycopy(array, position, array, 0, length);
      limit = length;
    }
    length = dataBuf.limit();
    System.arraycopy(dataBuf.array(), 0, array, limit, length);
    limit += length;
    buf.position(0);
    buf.limit(limit);
    return buf;
  }
}
