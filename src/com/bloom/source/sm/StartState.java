package com.bloom.source.sm;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;

public class StartState
  extends SMState
{
  StateMachine stateMachine;
  Property property;
  int colOffset;
  char[] coiArray;
  
  public StartState(StateMachine context, Property prop)
  {
    this.stateMachine = context;
    this.property = prop;
    this.colOffset = 0;
    this.coiArray = getCharsOfInterest().toCharArray();
  }
  
  public String getCharsOfInterest()
  {
    String ret = "";
    for (int idx = 0; idx < this.stateMachine.getStateStack().length; idx++)
    {
      String coi = this.stateMachine.getStateStack()[idx].getCharsOfInterest();
      ret = ret + coi;
    }
    return ret;
  }
  
  public Constant.status process(char inputChar)
  {
    this.stateMachine.setCurrentState(this);
    boolean foundInteresting = false;
    for (char c : this.coiArray) {
      if (c == inputChar)
      {
        foundInteresting = true;
        break;
      }
    }
    if (foundInteresting)
    {
      SMState[] stack = this.stateMachine.getStateStack();
      for (int index = 0; index < stack.length; index++)
      {
        Constant.status returnStatus = stack[index].canAccept(inputChar);
        if (returnStatus != Constant.status.NOT_ACCEPTED) {
          return returnStatus;
        }
      }
    }
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
  
  public void reset()
  {
    setCurrentState();
  }
}
