package com.bloom.source.nvp.sm;

import java.util.LinkedList;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.intf.State;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.nvp.NVPProperty;

public class NVPStateMachine
{
  NVPState[] stateObjStack;
  public LinkedList<Constant.status> eventQueue;
  NVPStartState startState;
  NVPQuotedCharState quotedCharState;
  NVPSpecialState specialState;
  NVPCommentCharState commentState = null;
  NVPDelimiterProcessingState colDelimiterState;
  NVPRowDelimiterProcessingState rowDelimiterState;
  NVPState previousState;
  NVPState currentState;
  Constant.status currentStatus;
  Property property;
  int type = 1;
  
  public NVPStateMachine(NVPProperty prop)
  {
    this.startState = new NVPStartState(this, prop);
    this.quotedCharState = new NVPQuotedCharState(this, prop);
    
    this.specialState = new NVPSpecialState(this, prop);
    
    this.commentState = new NVPCommentCharState(this, prop);
    this.colDelimiterState = new NVPDelimiterProcessingState(this, prop);
    this.rowDelimiterState = new NVPRowDelimiterProcessingState(this, prop);
    
    this.eventQueue = new LinkedList();
    this.stateObjStack = new NVPState[4];
    this.stateObjStack[0] = this.commentState;
    this.stateObjStack[1] = this.quotedCharState;
    this.stateObjStack[2] = this.rowDelimiterState;
    this.stateObjStack[3] = this.colDelimiterState;
    
    this.currentState = this.startState;
    this.previousState = this.currentState;
    this.property = prop;
  }
  
  public Constant.status process(char inputChar)
  {
    return this.currentState.process(inputChar);
  }
  
  public NVPState[] getStateStack()
  {
    return this.stateObjStack;
  }
  
  public NVPDelimiterProcessingState getDelimiterProcessingState()
  {
    return this.colDelimiterState;
  }
  
  public NVPRowDelimiterProcessingState getRowDelimiterProcessingState()
  {
    return this.rowDelimiterState;
  }
  
  public NVPStartState getStartState()
  {
    return this.startState;
  }
  
  public NVPQuotedCharState getQuotedCharState()
  {
    return this.quotedCharState;
  }
  
  public NVPCommentCharState getCommentCharState()
  {
    return this.commentState;
  }
  
  public State getCurrentState()
  {
    return this.currentState;
  }
  
  public Constant.status getCurrentStatus()
  {
    return this.currentStatus;
  }
  
  public NVPSpecialState getSpecialState()
  {
    return this.specialState;
  }
  
  public void setCurrentState(NVPState currentState)
  {
    this.currentState = currentState;
  }
  
  public void setCurrentStatus(Constant.status currentStatus)
  {
    this.currentStatus = currentStatus;
  }
  
  Constant.status getMode()
  {
    return this.currentStatus;
  }
  
  public void reset()
  {
    this.type = 1;
    setCurrentState(getStartState());
  }
}
