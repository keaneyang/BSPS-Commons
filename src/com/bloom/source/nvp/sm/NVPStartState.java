package com.bloom.source.nvp.sm;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;

public class NVPStartState
  extends NVPState
{
  NVPStateMachine stateMachine;
  Property property;
  int colOffset;
  
  NVPStartState(NVPStateMachine context, Property prop)
  {
    this.stateMachine = context;
    this.property = prop;
    this.colOffset = 0;
  }
  
  public Constant.status process(char inputChar)
  {
    setCurrentState();
    NVPState[] stack = this.stateMachine.getStateStack();
    for (int index = 0; index < stack.length; index++)
    {
      Constant.status returnStatus = stack[index].canAccept(inputChar);
      if (returnStatus != Constant.status.NOT_ACCEPTED) {
        return returnStatus;
      }
    }
    return publishDelayedEvent();
  }
  
  public Constant.status publishDelayedEvent()
  {
    return Constant.status.NORMAL;
  }
  
  public void setCurrentState()
  {
    if (this.stateMachine.currentState != this) {
      this.stateMachine.setCurrentState(this);
    }
  }
  
  public Constant.status canAccept(char inputChar)
  {
    return null;
  }
}
