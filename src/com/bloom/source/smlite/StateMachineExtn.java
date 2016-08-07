package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;
import com.bloom.source.lib.reader.Reader;

public class StateMachineExtn
  extends StateMachine
{
  public StateMachineExtn(Reader reader, SMProperty prop)
  {
    super(reader, prop);
  }
  
  public StateMachineExtn(SMProperty prop)
  {
    super(prop);
  }
  
  public StateMachineExtn(SMProperty _prop, SMCallback _callback)
  {
    super(_prop, _callback);
  }
  
  public CharParser createParser(SMProperty smProperty)
  {
    return new CharParserExtn(this, smProperty);
  }
  
  protected boolean validateProperty()
  {
    return true;
  }
}
