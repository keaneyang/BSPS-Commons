package com.bloom.source.smlite;

import org.apache.log4j.Logger;

public class ExpressionBuilder
{
  public static final String DEFAULT_EXPRESSION = "Expression";
  public static final String NUMERIC_EXPRESSION = "NumericExpression";
  static Logger logger = Logger.getLogger(ExpressionBuilder.class);
  
  static synchronized Expression buildExpression(CharParser parser, String delimiter, SMEvent eventObject)
  {
    Pattern pattern = PatternFactory.createPattern(delimiter);
    
    eventObject.length = delimiter.length();
    eventObject = pattern.updateEventAttribute(eventObject);
    String[] delList = pattern.getListOfPatterns();
    if (pattern.expressionType.equals("NumericExpression")) {
      return new NumericExpression(parser, delList, eventObject);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Using default expression for {" + delimiter + "}");
    }
    return new Expression(parser, delList, eventObject);
  }
}
