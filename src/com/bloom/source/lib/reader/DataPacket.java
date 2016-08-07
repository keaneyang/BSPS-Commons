package com.bloom.source.lib.reader;

public class DataPacket
{
  private Object data;
  private String id;
  
  public DataPacket(Object buffer, String identifier)
  {
    this.data = buffer;
    this.id = identifier;
  }
  
  public Object data()
  {
    return this.data;
  }
  
  public void data(Object buffer)
  {
    this.data = buffer;
  }
  
  public String id()
  {
    return this.id;
  }
  
  public void id(String identifier)
  {
    this.id = identifier;
  }
}
