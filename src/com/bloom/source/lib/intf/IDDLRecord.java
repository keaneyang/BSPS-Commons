package com.bloom.source.lib.intf;

public abstract interface IDDLRecord
{
  public abstract void setTimestamp(long paramLong);
  
  public abstract void setPosition(byte[] paramArrayOfByte);
  
  public abstract void setOperationName(String paramString);
  
  public abstract void setCommand(String paramString);
  
  public abstract void setCatalogObjectName(String paramString);
  
  public abstract void setCatalogObjectType(String paramString);
}
