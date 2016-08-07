package com.bloom.source.nvp.sm;

public class Delimiter
{
  private String delimiterString;
  private int offset;
  private boolean completedFlag;
  private boolean isMatched;
  
  public boolean isMatched()
  {
    return this.isMatched;
  }
  
  public boolean isCompleted()
  {
    return this.completedFlag;
  }
  
  public Delimiter(String str)
  {
    this.delimiterString = str;
    this.offset = 0;
    this.completedFlag = false;
    this.isMatched = false;
  }
  
  public boolean compare(char c)
  {
    if (this.completedFlag) {
      return this.isMatched;
    }
    if (c == this.delimiterString.charAt(this.offset))
    {
      this.offset += 1;
      if (this.delimiterString.length() == this.offset)
      {
        this.completedFlag = true;
        this.isMatched = true;
      }
      return true;
    }
    this.offset = 0;
    this.completedFlag = true;
    return false;
  }
  
  public void reset(boolean forceReset)
  {
    if ((this.completedFlag) || (forceReset))
    {
      this.offset = 0;
      this.completedFlag = false;
      this.isMatched = false;
    }
  }
  
  public int length()
  {
    return this.delimiterString.length();
  }
  
  public String getString()
  {
    return this.delimiterString;
  }
}
