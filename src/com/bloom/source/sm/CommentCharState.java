package com.bloom.source.sm;

import com.bloom.source.lib.constant.Constant;

public class CommentCharState
  extends SMState
{
  StateMachine stateMachine = null;
  SMProperty property = null;
  boolean isCharAcceptanaceCheck;
  boolean inCommentMode;
  
  CommentCharState(StateMachine stateMachine, SMProperty property)
  {
    this.stateMachine = stateMachine;
    this.property = property;
  }
  
  public String getCharsOfInterest()
  {
    return "" + this.property.commentcharacter;
  }
  
  public Constant.status process(char inputChar)
  {
    if (inputChar == this.property.commentcharacter)
    {
      this.stateMachine.previousState = this;
      this.stateMachine.setCurrentState(this.stateMachine.getStartState());
      this.inCommentMode = true;
      return Constant.status.IN_COMMENT;
    }
    if (this.isCharAcceptanaceCheck)
    {
      if (this.inCommentMode != true)
      {
        this.isCharAcceptanaceCheck = false;
        return Constant.status.NOT_ACCEPTED;
      }
      Constant.status st = this.stateMachine.rowDelimiterState.canAccept(inputChar);
      if (st == Constant.status.END_OF_ROW)
      {
        this.inCommentMode = false;
        this.stateMachine.setCurrentState(this.stateMachine.getStartState());
        return Constant.status.END_OF_COMMENT;
      }
      return Constant.status.IN_COMMENT;
    }
    this.stateMachine.setCurrentState(this.stateMachine.getStartState());
    return Constant.status.NORMAL;
  }
  
  public Constant.status canAccept(char inputChar)
  {
    this.isCharAcceptanaceCheck = true;
    return process(inputChar);
  }
  
  public void reset()
  {
    this.inCommentMode = false;
    this.isCharAcceptanaceCheck = false;
    this.stateMachine.previousState = this.stateMachine.startState;
    this.stateMachine.setCurrentState(this.stateMachine.startState);
  }
}
