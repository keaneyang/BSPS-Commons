package com.bloom.source.nvp.sm;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.constant.Constant.status;
import com.bloom.source.nvp.NVPProperty;

public class NVPCommentCharState
  extends NVPState
{
  NVPStateMachine stateMachine = null;
  NVPProperty property = null;
  boolean isCharAcceptanaceCheck;
  
  NVPCommentCharState(NVPStateMachine stateMachine, NVPProperty property)
  {
    this.stateMachine = stateMachine;
    this.property = property;
  }
  
  public status process(char inputChar)
  {
    if (inputChar == this.property.commentcharacter)
    {
      this.stateMachine.previousState = this;
      this.stateMachine.setCurrentState(this.stateMachine.getStartState());
      return Constant.status.IN_COMMENT;
    }
    if (this.isCharAcceptanaceCheck)
    {
      this.isCharAcceptanaceCheck = false;
      return Constant.status.NOT_ACCEPTED;
    }
    this.stateMachine.setCurrentState(this.stateMachine.getStartState());
    return Constant.status.NORMAL;
  }
  
  public status canAccept(char inputChar)
  {
    this.isCharAcceptanaceCheck = true;
    return process(inputChar);
  }
}
