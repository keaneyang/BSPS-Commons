package com.bloom.source.lib.reader;

import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.net.Socket;

public class WASocket
  extends ReaderBase
{
  protected Socket clientSocket;
  protected String serverIP;
  protected int serverPort;
  protected int listenPort;
  
  public WASocket(ReaderBase link)
    throws AdapterException
  {
    super(link);
  }
  
  protected WASocket(Property prop)
    throws AdapterException
  {
    super(prop);
  }
  
  protected void init()
    throws AdapterException
  {
    super.init();
  }
  
  public Object readBlock()
    throws AdapterException
  {
    return null;
  }
  
  public void close()
    throws IOException
  {}
  
  public void connect()
    throws AdapterException
  {}
}
