package com.bloom.source.lib.meta;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.constant.Constant.fieldType;

import java.nio.ByteBuffer;

public class Column
{
  int index;
  String name;
  Constant.fieldType type;
  int size;
  int lengthOffset;
  
  public int getIndex()
  {
    return this.index;
  }
  
  public void setIndex(int index)
  {
    this.index = index;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public void setName(String name)
  {
    this.name = name;
  }
  
  public Constant.fieldType getType()
  {
    return this.type;
  }
  
  public void setType(Constant.fieldType type)
  {
    this.type = type;
  }
  
  public int getSize()
  {
    return this.size;
  }
  
  public void setSize(int size)
  {
    this.size = size;
  }
  
  public void lengthOffset(int noOfBytes)
  {
    this.lengthOffset = noOfBytes;
  }
  
  public int lengthOffset()
  {
    return this.lengthOffset;
  }
  
  public Object getValue(byte[] rowData, int offset, int length)
  {
    return "";
  }
  
  public int getLengthOfString(ByteBuffer buffer, int stringColumnLength)
  {
    return -1;
  }
}
