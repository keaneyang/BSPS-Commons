package com.bloom.source.lib.utils;

import java.io.IOException;
import java.io.InputStream;

class UTF_32BE
  extends StandardCharset
{
  public boolean skipBOM(InputStream in)
    throws IOException
  {
    byte[] bom = new byte[4];
    in.read(bom, 0, bom.length);
    if ((bom[0] == 0) && (bom[1] == 0) && (bom[2] == -2) && (bom[3] == -1)) {
      return true;
    }
    return false;
  }
}
