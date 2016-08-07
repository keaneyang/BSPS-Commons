package com.bloom.source.lib.intf;

import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.columntype;

public abstract interface IMetadataRecord
{
  public abstract void setTimestamp(long paramLong)
    throws TransactionManagerException;
  
  public abstract void setLSN(byte[] paramArrayOfByte)
    throws TransactionManagerException;
  
  public abstract void setTransactionId(String paramString)
    throws TransactionManagerException;
  
  public abstract void setMetadataRecordId(String paramString)
    throws TransactionManagerException;
  
  public abstract void setVersion(String paramString)
    throws TransactionManagerException;
  
  public abstract void setSchemaName(String paramString)
    throws TransactionManagerException;
  
  public abstract void setDatabaseName(String paramString)
    throws TransactionManagerException;
  
  public abstract void setTableName(String paramString)
    throws TransactionManagerException;
  
  public abstract void setColumnCount(int paramInt)
    throws TransactionManagerException;
  
  public abstract void setColumnType(columntype paramcolumntype, int paramInt)
    throws TransactionManagerException;
  
  public abstract void setColumnIsNullable(int paramInt)
    throws TransactionManagerException;
  
  public abstract void setKeyColumn(int paramInt)
    throws TransactionManagerException;
  
  public abstract void setColumnPrecision(int paramInt1, int paramInt2)
    throws TransactionManagerException;
  
  public abstract void setColumnScale(int paramInt1, int paramInt2)
    throws TransactionManagerException;
  
  public abstract void setColumnName(String paramString, int paramInt)
    throws TransactionManagerException;
  
  public abstract void setColumnSize(int paramInt1, int paramInt2)
    throws TransactionManagerException;
}

