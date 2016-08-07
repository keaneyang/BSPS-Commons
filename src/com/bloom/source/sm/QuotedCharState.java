package com.bloom.source.sm;

import com.bloom.source.lib.constant.Constant;

public class QuotedCharState
  extends SMState
{
  StateMachine stateMachine;
  SMProperty property;
  boolean isCharAcceptanaceCheck;
  boolean inQuoteBlock;
  QuoteSet[] quoteList;
  QuoteSet matchedQuoteSet;
  QuoteSet lastMatchedQuoteSet;
  boolean ignoreRowDelimiterInQuote;
  char[] coiArray;
  
  public QuotedCharState(StateMachine context, SMProperty prop)
  {
    this.stateMachine = context;
    this.property = prop;
    this.quoteList = null;
    this.matchedQuoteSet = null;
    this.lastMatchedQuoteSet = null;
    
    this.ignoreRowDelimiterInQuote = prop.ignoreRowDelimiterInQueue;
    String[] qList = this.property.quoteSetList;
    if (qList.length > 0)
    {
      this.quoteList = new QuoteSet[qList.length];
      for (int idx = 0; idx < qList.length; idx++) {
        this.quoteList[idx] = new QuoteSet(qList[idx]);
      }
    }
    this.coiArray = getCharsOfInterest().toCharArray();
  }
  
  public String getCharsOfInterest()
  {
    String ret = "";
    for (int idx = 0; idx < this.quoteList.length; idx++) {
      ret = ret + this.quoteList[idx].getQuoteAsString();
    }
    return ret;
  }
  
  public Constant.status process(char inputChar)
  {
    if (this.quoteList != null)
    {
      if (this.matchedQuoteSet != null)
      {
        if (!this.ignoreRowDelimiterInQuote)
        {
          Constant.status st = this.stateMachine.getRowDelimiterProcessingState().process(inputChar);
          this.stateMachine.setCurrentState(this);
          if (st == Constant.status.END_OF_ROW)
          {
            reset();
            return st;
          }
        }
        QuoteSet.QuoteState state = this.matchedQuoteSet.match(inputChar);
        if (state == QuoteSet.QuoteState.END)
        {
          this.lastMatchedQuoteSet = this.matchedQuoteSet;
          this.matchedQuoteSet = null;
          if (this.inQuoteBlock)
          {
            this.inQuoteBlock = false;
            this.stateMachine.setCurrentState(this.stateMachine.startState);
          }
          else
          {
            System.out.println("Something wrong in our processing");
          }
        }
      }
      else
      {
        boolean foundInteresting = false;
        for (char c : this.coiArray) {
          if (c == inputChar)
          {
            foundInteresting = true;
            break;
          }
        }
        if (foundInteresting) {
          for (int idx = 0; idx < this.quoteList.length; idx++)
          {
            QuoteSet.QuoteState state = this.quoteList[idx].match(inputChar);
            if (state != QuoteSet.QuoteState.NOT_MATCHED) {
              if (state == QuoteSet.QuoteState.BEGIN)
              {
                this.matchedQuoteSet = this.quoteList[idx];
                this.inQuoteBlock = true;
                this.stateMachine.setCurrentState(this);
                break;
              }
            }
          }
        }
      }
      if (this.isCharAcceptanaceCheck != true) {
        return Constant.status.NORMAL;
      }
      this.isCharAcceptanaceCheck = false;
      if (this.inQuoteBlock)
      {
        if (this.stateMachine.colDelimiterState.hasEventTobePublished()) {
          return this.stateMachine.colDelimiterState.getStatusToPublish();
        }
        if (this.stateMachine.rowDelimiterState.hasEventTobePublished()) {
          return this.stateMachine.rowDelimiterState.getStatusToPublish();
        }
        return Constant.status.NORMAL;
      }
      return Constant.status.NOT_ACCEPTED;
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
  
  public void reset()
  {
    this.matchedQuoteSet = null;
    this.lastMatchedQuoteSet = null;
    this.inQuoteBlock = false;
    this.isCharAcceptanaceCheck = false;
    for (int idx = 0; idx < this.quoteList.length; idx++) {
      this.quoteList[idx].reset();
    }
  }
}
