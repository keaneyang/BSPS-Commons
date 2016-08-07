package com.bloom.source.lib.intf;

import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.controltype;

public abstract interface IControlRecord
{
  public abstract void setTimestamp(long paramLong)
    throws TransactionManagerException;
  
  public abstract void setLSN(byte[] paramArrayOfByte)
    throws TransactionManagerException;
  
  public abstract void setTransactionId(byte[] paramArrayOfByte)
    throws TransactionManagerException;
  
  public abstract void setControlType(controltype paramcontroltype)
    throws TransactionManagerException;
  
  public abstract void setTransactionUserId(String paramString)
    throws TransactionManagerException;
  
  public abstract void setCommand(String paramString)
    throws TransactionManagerException;
}

