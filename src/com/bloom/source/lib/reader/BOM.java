package com.bloom.source.lib.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

class BOM
{
  private static Logger logger = Logger.getLogger(BOM.class);
  protected byte[] pattern;
  protected byte[] input;
  private static Map<String, BOM> bomClassMap;
  
  static synchronized void init()
  {
    if (bomClassMap == null)
    {
      bomClassMap = new HashMap();
      bomClassMap.put("UTF-8", new UTF8Bom());
      bomClassMap.put("UTF-16BE", new UTF16BEBom());
      bomClassMap.put("UTF-16LE", new UTF16LEBom());
      bomClassMap.put("UTF-32BE", new UTF32BEBom());
      bomClassMap.put("UTF-32LE", new UTF32LEBom());
    }
  }
  
  static void skip(String charset, ByteBuffer buffer)
    throws IOException
  {
    String charSet = null;
    if ((charset == null) || (charset.isEmpty())) {
      charSet = "UTF-8";
    } else {
      charSet = charset;
    }
    BOM tmp = (BOM)bomClassMap.get(charSet.toUpperCase());
    if (tmp != null) {
      tmp.skipBOM(buffer);
    } else if (logger.isDebugEnabled()) {
      logger.debug("Unrecoganized charset [" + charset + "] ignoring BOM");
    }
  }
  
  public void skipBOM(ByteBuffer in)
    throws IOException
  {
    this.input = new byte[this.pattern.length];
    System.arraycopy(in.array(), 0, this.input, 0, this.pattern.length);
    if (Arrays.equals(this.input, this.pattern)) {
      in.position(this.pattern.length);
    }
  }
}
