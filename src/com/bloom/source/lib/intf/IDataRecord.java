package com.bloom.source.lib.intf;

import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.columntype;
import com.bloom.source.lib.type.operationtype;

public abstract interface IDataRecord
{
  public abstract void setTimestamp(long paramLong)
    throws TransactionManagerException;
  
  public abstract void setLSN(byte[] paramArrayOfByte)
    throws TransactionManagerException;
  
  public abstract void setTransactionId(byte[] paramArrayOfByte)
    throws TransactionManagerException;
  
  public abstract void setMetaRecordID(String paramString)
    throws TransactionManagerException;
  
  public abstract void setOperationType(operationtype paramoperationtype)
    throws TransactionManagerException;
  
  public abstract void setColumnCount(int paramInt)
    throws TransactionManagerException;
  
  public abstract void setColumnValueBefore(int paramInt, Object paramObject, columntype paramcolumntype)
    throws TransactionManagerException;
  
  public abstract void setColumnValueAsStringBefore(int paramInt, String paramString)
    throws TransactionManagerException;
  
  public abstract void setColumnValueAsNullBefore(int paramInt)
    throws TransactionManagerException;
  
  public abstract void setColumnValueAfter(int paramInt, Object paramObject, columntype paramcolumntype)
    throws TransactionManagerException;
  
  public abstract void setColumnValueAsStringAfter(int paramInt, String paramString)
    throws TransactionManagerException;
  
  public abstract void setColumnValueAsNullAfter(int paramInt)
    throws TransactionManagerException;
  
  public abstract void setParameterValue(String paramString1, String paramString2)
    throws TransactionManagerException;
}
