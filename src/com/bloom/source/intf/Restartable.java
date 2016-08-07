package com.bloom.source.intf;

public abstract interface Restartable
{
  public abstract boolean isRestartable();
  
  public abstract boolean restart();
  
  public abstract void setPosition(Object paramObject);
  
  public abstract Object getLastPosition();
}


