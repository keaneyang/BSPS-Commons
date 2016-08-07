package com.bloom.source.sm;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;

public class SpecialState
  extends StartState
{
  StateMachine stateMachine;
  Property property;
  boolean isCharAcceptanaceCheck;
  
  public SpecialState(StateMachine context, Property prop)
  {
    super(context, prop);
    this.stateMachine = context;
    this.property = prop;
  }
  
  public Constant.status process(char inputChar)
  {
    Constant.status s = super.process(inputChar);
    if (s == Constant.status.NORMAL)
    {
      this.stateMachine.setCurrentState(this.stateMachine.getStartState());
      if (this.stateMachine.colDelimiterState.hasEventTobePublished()) {
        return this.stateMachine.colDelimiterState.getStatusToPublish();
      }
      if (this.stateMachine.rowDelimiterState.hasEventTobePublished()) {
        return this.stateMachine.rowDelimiterState.getStatusToPublish();
      }
      return Constant.status.NORMAL;
    }
    return s;
  }
  
  public Constant.status canAccept(char inputChar)
  {
    this.isCharAcceptanaceCheck = true;
    return process(inputChar);
  }
  
  public void reset() {}
}
