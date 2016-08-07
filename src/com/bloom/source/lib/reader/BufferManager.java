package com.bloom.source.lib.reader;

import com.bloom.common.exc.AdapterException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class BufferManager
  extends ReaderBase
{
  private Map<String, Object> bufferMap;
  
  public BufferManager(ReaderBase strategy)
    throws AdapterException
  {
    super(strategy);
    init();
  }
  
  public void init()
    throws AdapterException
  {
    super.init();
    this.bufferMap = new TreeMap();
    this.supportsMutipleEndpoint = true;
  }
  
  public Object readBlock()
    throws AdapterException
  {
    return this.linkedStrategy.readBlock();
  }
  
  public Object readBlock(boolean multiEndpointSupport)
    throws AdapterException
  {
    Object obj = this.linkedStrategy.readBlock(true);
    if (obj != null) {
      return appendData((DataPacket)obj);
    }
    return null;
  }
  
  protected Object allocateInitialBuffer()
  {
    return ByteBuffer.allocate(blockSize() * 2).flip();
  }
  
  public Object appendData(DataPacket packet)
  {
    String id = packet.id();
    Object obj = this.bufferMap.get(id);
    if (obj == null)
    {
      Object bufferObj = allocateInitialBuffer();
      this.bufferMap.put(id, bufferObj);
      obj = bufferObj;
    }
    return appendData(obj, packet.data());
  }
  
  protected Object appendData(Object obj, Object data)
  {
    ByteBuffer buf = (ByteBuffer)obj;
    ByteBuffer dataBuf = (ByteBuffer)data;
    
    int position = buf.position();
    int limit = buf.limit();
    int length = limit - position;
    
    byte[] array = buf.array();
    if (position > 0)
    {
      System.arraycopy(array, position, array, 0, length);
      position = length;
    }
    length = position + dataBuf.limit();
    System.arraycopy(dataBuf.array(), 0, array, position, limit);
    buf.position(0);
    buf.limit(limit);
    return buf;
  }
  
  public boolean supportsMutipleEndpoint()
  {
    return this.supportsMutipleEndpoint;
  }
}
