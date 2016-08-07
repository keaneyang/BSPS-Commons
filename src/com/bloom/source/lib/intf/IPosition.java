package com.bloom.source.lib.intf;

import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.positiontype;

public abstract interface IPosition
{
  public abstract positiontype getPositionType()
    throws TransactionManagerException;
  
  public abstract byte[] getPositionValue()
    throws TransactionManagerException;
}

