package com.bloom.source.smlite;

import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.common.exc.AdapterException;

public class StateMachineBuilder
{
  private static final String LITE = "Lite";
  private static final String EXTN = "Extn";
  private static final String PATTERN = "Patt";
  SMProperty smProperty;
  
  public StateMachineBuilder(SMProperty prop)
  {
    this.smProperty = prop;
  }
  
  private boolean hasMultiCharacterDelimiter()
  {
    if (checkForMultiCharacter(this.smProperty.timeStamp)) {
      return true;
    }
    if (checkForMultiCharacter(this.smProperty.escapeList)) {
      return true;
    }
    if (checkForMultiCharacter(this.smProperty.columnDelimiterList)) {
      return true;
    }
    if (checkForMultiCharacter(this.smProperty.rowDelimiterList)) {
      return true;
    }
    if (checkForMultiCharacter(this.smProperty.recordBegin)) {
      return true;
    }
    return false;
  }
  
  private boolean checkForMultiCharacter(String[] delimiter)
  {
    for (int itr = 0; (delimiter != null) && (itr < delimiter.length); itr++) {
      if (delimiter[itr].length() > 1) {
        return true;
      }
    }
    return false;
  }
  
  private boolean hasRegExDelimiter()
  {
    return false;
  }
  
  private StateMachine createStateMachine(String smName, Reader dataSource, Property prop)
  {
    StateMachine sm = null;
    if (smName.equals("Lite")) {
      sm = new StateMachine(dataSource, new SMProperty(prop.getMap()));
    } else if (smName.equals("Extn")) {
      sm = new StateMachineExtn(dataSource, new SMProperty(prop.getMap()));
    }
    return sm;
  }
  
  private CharParser createParser(StateMachine sm, String parserName)
    throws AdapterException
  {
    CharParser parser = null;
    if (parserName.equals("Lite")) {
      parser = new CharContinousHashParser(sm, this.smProperty);
    } else if (parserName.equals("Extn")) {
      try
      {
        parser = new CharContinousHashParser(sm, this.smProperty);
      }
      catch (Exception e)
      {
        parser = new CharParserExtn(sm, this.smProperty);
      }
    } else {
      throw new AdapterException("Unsupported parser [" + parserName + "]");
    }
    return parser;
  }
  
  public StateMachine createStateMachine(Reader reader, Property prop)
    throws AdapterException
  {
    String stateMachine;
    String charParser;
    if (hasRegExDelimiter())
    {
       charParser = "Patt";
      stateMachine = "Extn";
    }
    else
    {
      if (!hasMultiCharacterDelimiter())
      {
         charParser = "Lite";
        stateMachine = "Lite";
      }
      else
      {
        charParser = "Extn";
        stateMachine = "Extn";
      }
    }
    StateMachine sm = createStateMachine(stateMachine, reader, prop);
    if (sm != null)
    {
      sm.init();
      CharParser parser = createParser(sm, charParser);
      if (parser != null)
      {
        parser.init();
        sm.parser(parser);
        return sm;
      }
    }
    throw new AdapterException("Couldn't create appropriate state machine for the given property");
  }
}
