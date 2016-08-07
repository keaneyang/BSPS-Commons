package com.bloom.source.sm;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.constant.Constant.status;
import com.bloom.source.lib.intf.State;
import com.bloom.source.lib.prop.Property;

import java.util.LinkedList;

public class StateMachine
{
  SMState[] stateObjStack;
  public LinkedList<Constant.status> eventQueue;
  StartState startState;
  QuotedCharState quotedCharState;
  SpecialState specialState;
  CommentCharState commentState = null;
  DelimiterProcessingState colDelimiterState;
  RowDelimiterProcessingState rowDelimiterState;
  SMState previousState;
  SMState currentState;
  Constant.status currentStatus;
  Property property;
  int type = 1;
  
  public StateMachine(SMProperty prop)
  {
    this.quotedCharState = new QuotedCharState(this, prop);
    this.commentState = new CommentCharState(this, prop);
    this.colDelimiterState = new DelimiterProcessingState(this, prop);
    this.rowDelimiterState = new RowDelimiterProcessingState(this, prop);
    
    this.eventQueue = new LinkedList();
    this.stateObjStack = new SMState[4];
    this.stateObjStack[0] = this.colDelimiterState;
    this.stateObjStack[1] = this.quotedCharState;
    this.stateObjStack[2] = this.rowDelimiterState;
    this.stateObjStack[3] = this.commentState;
    
    this.startState = new StartState(this, prop);
    this.specialState = new SpecialState(this, prop);
    
    this.currentState = this.startState;
    this.previousState = this.currentState;
    this.property = prop;
  }
  
  public Constant.status process(char inputChar)
  {
    return this.currentState.process(inputChar);
  }
  
  public SMState[] getStateStack()
  {
    return this.stateObjStack;
  }
  
  public DelimiterProcessingState getDelimiterProcessingState()
  {
    return this.colDelimiterState;
  }
  
  public RowDelimiterProcessingState getRowDelimiterProcessingState()
  {
    return this.rowDelimiterState;
  }
  
  public StartState getStartState()
  {
    return this.startState;
  }
  
  public QuotedCharState getQuotedCharState()
  {
    return this.quotedCharState;
  }
  
  public CommentCharState getCommentCharState()
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
  
  public SpecialState getSpecialState()
  {
    return this.specialState;
  }
  
  public void setCurrentState(SMState currentState)
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
    
    this.eventQueue.clear();
    for (int idx = 0; idx < this.stateObjStack.length; idx++) {
      this.stateObjStack[idx].reset();
    }
  }
  
  public String matchedColDelimiter()
  {
    return this.colDelimiterState.matchedDelimiter();
  }
  
  public String matchedRowDelimiter()
  {
    return this.rowDelimiterState.matchedDelimiter();
  }
  
  public QuoteSet matchedQuoteSet()
  {
    return this.quotedCharState.lastMatchedQuoteSet;
  }
}
