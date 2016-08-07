package com.bloom.source.lib.utils;

import java.io.IOException;
import java.io.InputStream;

class UTF_8
  extends StandardCharset
{
  public boolean skipBOM(InputStream in)
    throws IOException
  {
    byte[] bom = new byte[3];
    in.read(bom, 0, bom.length);
    if ((bom[0] == -17) && (bom[1] == -69) && (bom[2] == -65)) {
      return true;
    }
    return false;
  }
}
