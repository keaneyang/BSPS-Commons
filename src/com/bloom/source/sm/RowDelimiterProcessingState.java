package com.bloom.source.sm;

import com.bloom.source.lib.constant.Constant;

public class RowDelimiterProcessingState
  extends DelimiterProcessingState
{
  Delimiter[] delimiterList;
  
  public RowDelimiterProcessingState(StateMachine stateMachine, SMProperty prop)
  {
    super(stateMachine, prop);
    
    String[] rList = prop.rowDelimiterList;
    this.delimiterList = new Delimiter[rList.length];
    for (int idx = 0; idx < this.delimiterList.length; idx++) {
      this.delimiterList[idx] = new Delimiter(rList[idx]);
    }
  }
  
  public String getCharsOfInterest()
  {
    String ret = "";
    for (int idx = 0; idx < this.delimiterList.length; idx++) {
      ret = ret + this.delimiterList[idx].getString();
    }
    return ret;
  }
  
  public Constant.status process(char inputChar)
  {
    boolean delimiterFound = false;
    boolean doneWithValidation = false;
    for (int idx = 0; idx < this.delimiterList.length; idx++) {
      if ((this.delimiterList[idx].compare(inputChar)) && 
        (this.delimiterList[idx].isCompleted() == true))
      {
        doneWithValidation = true;
        if (this.delimiterList[idx].isMatched())
        {
          delimiterFound = true;
          this.matchedDelimiter = this.delimiterList[idx];
          break;
        }
      }
    }
    for (int idx = 0; idx < this.delimiterList.length; idx++) {
      this.delimiterList[idx].reset(false);
    }
    if (doneWithValidation)
    {
      if (delimiterFound)
      {
        for (int idx = 0; idx < this.delimiterList.length; idx++) {
          this.delimiterList[idx].reset(true);
        }
        if (this.stateMachine.currentState == this.stateMachine.getSpecialState())
        {
          if (this.stateMachine.getDelimiterProcessingState().hasEventTobePublished()) {
            this.stateMachine.eventQueue.add(Constant.status.END_OF_COLUMN);
          }
          this.stateMachine.eventQueue.add(Constant.status.END_OF_ROW);
          return Constant.status.MULTIPLE_STATUS;
        }
        this.stateMachine.setCurrentState(this.stateMachine.previousState);
        return getStatusToPublish();
      }
      if (this.isCanAcceptanaceCheck)
      {
        this.isCanAcceptanaceCheck = false;
        return Constant.status.NOT_ACCEPTED;
      }
      this.stateMachine.setCurrentState(this.stateMachine.previousState);
      return Constant.status.NORMAL;
    }
    if (this.isCanAcceptanaceCheck)
    {
      this.isCanAcceptanaceCheck = false;
      return Constant.status.NOT_ACCEPTED;
    }
    this.stateMachine.setCurrentState(this.stateMachine.getStartState());
    return Constant.status.NORMAL;
  }
  
  public Constant.status getStatusToPublish()
  {
    return Constant.status.END_OF_ROW;
  }
  
  public int getMatchedDelimiterLength()
  {
    return this.matchedDelimiter.length();
  }
  
  public boolean isEscapeSequence()
  {
    return false;
  }
  
  public void reset()
  {
    this.matchedDelimiter = null;
    for (int idx = 0; idx < this.delimiterList.length; idx++) {
      this.delimiterList[idx].reset(true);
    }
  }
}
