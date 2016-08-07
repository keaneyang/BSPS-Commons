package com.bloom.source.lib.intf;

import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.sessiontype;

import java.util.Map;

public abstract interface ISession
{
  public abstract sessiontype getSessionType()
    throws TransactionManagerException;
  
  public abstract IPosition getPosition()
    throws TransactionManagerException;
  
  public abstract String[] getTableList()
    throws TransactionManagerException;
  
  public abstract String[] getExcludedTableList()
    throws TransactionManagerException;
  
  public abstract String[] getColumns(String paramString)
    throws TransactionManagerException;
  
  public abstract String[] getExcludedColumns(String paramString)
    throws TransactionManagerException;
  
  public abstract String getParameterValue(String paramString)
    throws TransactionManagerException;
  
  public abstract Map<String, Object> getPropertyMap()
    throws TransactionManagerException;
}

