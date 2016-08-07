package com.bloom.source.lib.utils;

import java.io.IOException;
import java.io.InputStream;

class UTF_16BE
  extends StandardCharset
{
  public boolean skipBOM(InputStream in)
    throws IOException
  {
    byte[] bom = new byte[2];
    in.read(bom, 0, bom.length);
    if ((bom[0] == -2) && (bom[1] == -1)) {
      return true;
    }
    return false;
  }
}
