package com.bloom.source.nvp.sm;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;

public class NVPSpecialState
  extends NVPStartState
{
  NVPStateMachine stateMachine;
  Property property;
  boolean isCharAcceptanaceCheck;
  
  NVPSpecialState(NVPStateMachine context, Property prop)
  {
    super(context, prop);
    this.stateMachine = context;
    this.property = prop;
  }
  
  public Constant.status publishDelayedEvent()
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
  
  public Constant.status canAccept(char inputChar)
  {
    this.isCharAcceptanaceCheck = true;
    return process(inputChar);
  }
}
