package com.bloom.source.lib.intf;

import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.recordstatus;

public abstract interface IStatusRecord
{
  public abstract void setname(String paramString)
    throws TransactionManagerException;
  
  public abstract void setIntegerMessage(int paramInt)
    throws TransactionManagerException;
  
  public abstract void setStatusType(recordstatus paramrecordstatus)
    throws TransactionManagerException;
  
  public abstract void setParameterValue(String paramString1, String paramString2)
    throws TransactionManagerException;
}

