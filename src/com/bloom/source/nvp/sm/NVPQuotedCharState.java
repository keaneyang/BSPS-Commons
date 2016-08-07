package com.bloom.source.nvp.sm;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;

public class NVPQuotedCharState
  extends NVPState
{
  NVPStateMachine stateMachine;
  Property property;
  boolean isCharAcceptanaceCheck;
  
  NVPQuotedCharState(NVPStateMachine context, Property prop)
  {
    this.stateMachine = context;
    this.property = prop;
  }
  
  public Constant.status process(char inputChar)
  {
    if (inputChar == this.property.quotecharacter)
    {
      this.stateMachine.setCurrentState(this.stateMachine.getStartState());
      
      return Constant.status.NORMAL;
    }
    if (this.isCharAcceptanaceCheck)
    {
      this.isCharAcceptanaceCheck = false;
      return Constant.status.NOT_ACCEPTED;
    }
    return Constant.status.NORMAL;
  }
  
  public Constant.status canAccept(char inputChar)
  {
    this.isCharAcceptanaceCheck = true;
    return process(inputChar);
  }
}
