package com.bloom.source.lib.intf;

import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.recordtype;

public abstract interface IRecord
{
  public abstract void setRecordType(recordtype paramrecordtype)
    throws TransactionManagerException;
  
  public abstract IControlRecord getControlRecord()
    throws TransactionManagerException;
  
  public abstract IDataRecord getDataRecord()
    throws TransactionManagerException;
  
  public abstract void readFromBytes(byte[] paramArrayOfByte)
    throws TransactionManagerException;
  
  public abstract IDDLRecord getCDCDDLRecord()
    throws TransactionManagerException;
}

