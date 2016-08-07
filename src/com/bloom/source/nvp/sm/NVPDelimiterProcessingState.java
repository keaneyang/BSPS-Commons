package com.bloom.source.nvp.sm;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;

public class NVPDelimiterProcessingState
  extends NVPState
{
  protected String delimiter;
  protected int delimiterOffset;
  NVPStateMachine stateMachine;
  boolean isCanAcceptanaceCheck;
  boolean delayEventPublish;
  int occurance = 0;
  byte[] tmp;
  Delimiter[] delimiterList;
  Delimiter matchedDelimiter;
  
  public NVPDelimiterProcessingState(NVPStateMachine stateMachine, Property prop)
  {
    this.stateMachine = stateMachine;
    this.delayEventPublish = false;
    this.occurance = 0;
    
    String[] tmpDel = prop.nvpvaluedelimiter;
    this.delimiterList = new Delimiter[tmpDel.length];
    for (int idx = 0; idx < tmpDel.length; idx++) {
      this.delimiterList[idx] = new Delimiter(tmpDel[idx]);
    }
  }
  
  public Constant.status process(char inputChar)
  {
    boolean delimiterFound = false;
    boolean doneWithValidation = false;
    boolean escapeThisDelimiter = false;
    for (int idx = 0; idx < this.delimiterList.length; idx++) {
      if ((this.delimiterList[idx].compare(inputChar)) && 
        (this.delimiterList[idx].isCompleted() == true))
      {
        doneWithValidation = true;
        if (this.delimiterList[idx].isMatched())
        {
          delimiterFound = true;
          if (isEscapeSequence(idx)) {
            escapeThisDelimiter = true;
          }
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
        this.stateMachine.setCurrentState(this.stateMachine.getSpecialState());
        for (int idx = 0; idx < this.delimiterList.length; idx++) {
          this.delimiterList[idx].reset(true);
        }
        if (escapeThisDelimiter)
        {
          this.delayEventPublish = false;
          return Constant.status.NORMAL;
        }
        this.delayEventPublish = true;
        return Constant.status.NORMAL;
      }
      escapeThisDelimiter = false;
      this.matchedDelimiter = null;
      if (this.isCanAcceptanaceCheck)
      {
        this.isCanAcceptanaceCheck = false;
        return Constant.status.NOT_ACCEPTED;
      }
      this.stateMachine.setCurrentState(this.stateMachine.previousState);
      return Constant.status.NORMAL;
    }
    this.matchedDelimiter = null;
    if (this.isCanAcceptanaceCheck)
    {
      this.isCanAcceptanaceCheck = false;
      return Constant.status.NOT_ACCEPTED;
    }
    this.stateMachine.setCurrentState(this.stateMachine.getStartState());
    return Constant.status.NORMAL;
  }
  
  public boolean isEscapeSequence(int index)
  {
    if (this.delimiterList[index] == this.matchedDelimiter) {
      return true;
    }
    return false;
  }
  
  public Constant.status getStatusToPublish()
  {
    return Constant.status.END_OF_COLUMN;
  }
  
  public boolean hasEventTobePublished()
  {
    if (this.delayEventPublish)
    {
      this.delayEventPublish = false;
      return true;
    }
    return false;
  }
  
  public Constant.status canAccept(char inputChar)
  {
    this.isCanAcceptanaceCheck = true;
    return process(inputChar);
  }
  
  public int getMatchedDelimiterLength()
  {
    return this.delimiter.length();
  }
}
