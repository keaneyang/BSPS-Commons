package com.bloom.source.lib.intf;

import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.acktype;
import com.bloom.source.lib.type.recordstatus;

public abstract interface ITransactionManager
{
  public abstract void init(ISession paramISession)
    throws TransactionManagerException;
  
  public abstract String getNextMetadataRecordID()
    throws TransactionManagerException;
  
  public abstract boolean getMetadataRecord(String paramString, IMetadataRecord paramIMetadataRecord)
    throws TransactionManagerException;
  
  public abstract recordstatus getRecord(IRecord paramIRecord)
    throws TransactionManagerException;
  
  public abstract void acknowledge(acktype paramacktype, IPosition paramIPosition)
    throws TransactionManagerException;
  
  public abstract void close()
    throws TransactionManagerException;
}

