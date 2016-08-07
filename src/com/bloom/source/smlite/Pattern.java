package com.bloom.source.smlite;

import org.apache.log4j.Logger;

public class Pattern
  implements Cloneable
{
  String expressionType = "";
  String pattern;
  private static final Logger logger = Logger.getLogger(Pattern.class);
  
  public void init(String pattern)
  {
    this.pattern = pattern;
    this.expressionType = "Expression";
  }
  
  public SMEvent updateEventAttribute(SMEvent event)
  {
    return event;
  }
  
  public String[] getListOfPatterns()
  {
    return new String[] { this.pattern };
  }
  
  public Object clone()
  {
    Object clone = null;
    try
    {
      clone = super.clone();
    }
    catch (CloneNotSupportedException e)
    {
      logger.warn("CloneNotSupportedException got while cloning Pattern object ", e);
    }
    return clone;
  }
}
