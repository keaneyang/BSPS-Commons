package com.bloom.source.lib.exc;

import java.io.Serializable;
import java.text.DecimalFormat;

public class GGSourceException
  extends Exception
  implements Serializable
{
  private static final long serialVersionUID = 178882191744299616L;
  private static final String GGCODE = "GG Source: ";
  private Type type;
  
  public static enum Type
  {
    GENERIC_EXCEPTION(100, "Unexpected Exception"),  NOT_IMPLEMENTED(101, "Method Not Implemented"),  END_OF_TRAIL(102, "End of Trail"),  IO_ISSUE(103, "Read Error"),  INVALID_RECORD(104, "Read Position Invalid"),  CHECKPOINT_ERROR(105, "Checkpointing Error");
    
    int type;
    String text;
    
    private Type(int type, String text)
    {
      this.type = type;
      this.text = text;
    }
    
    public String toString()
    {
      return this.type + ":" + this.text;
    }
  }
  
  DecimalFormat df = new DecimalFormat("0000");
  
  public GGSourceException()
  {
    this(Type.GENERIC_EXCEPTION);
  }
  
  public GGSourceException(String message)
  {
    this(Type.GENERIC_EXCEPTION, message);
  }
  
  public GGSourceException(Type type)
  {
    this.type = type;
  }
  
  public GGSourceException(Type type, String message)
  {
    super(message);
    this.type = type;
  }
  
  public GGSourceException(Type type, Throwable cause)
  {
    super(cause);
    this.type = type;
  }
  
  public GGSourceException(Type type, String message, Throwable cause)
  {
    super(message, cause);
    this.type = type;
  }
  
  public String getCode()
  {
    return "GG Source: " + this.type;
  }
  
  public String getMessage()
  {
    if (super.getMessage() != null) {
      return this.type.text + "\n" + super.getMessage();
    }
    return this.type.text;
  }
  
  public Type getType()
  {
    return this.type;
  }
  
  public String toString()
  {
    return "Exception in GoldenGate Trail Source: " + getCode() + "\n" + getMessage() + "\n";
  }
}
