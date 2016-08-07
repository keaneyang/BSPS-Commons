package com.bloom.source.lib.intf;

import com.bloom.source.lib.exc.TransactionManagerException;

public abstract interface TransactionManagerFactory
{
  public abstract ITransactionManager createInstance()
    throws TransactionManagerException;
  
  public abstract void destroyInstance()
    throws TransactionManagerException;
}

