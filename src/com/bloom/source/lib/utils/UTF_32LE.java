package com.bloom.source.lib.utils;

import java.io.IOException;
import java.io.InputStream;

class UTF_32LE
  extends StandardCharset
{
  public boolean skipBOM(InputStream in)
    throws IOException
  {
    byte[] bom = new byte[4];
    in.read(bom, 0, bom.length);
    if ((bom[0] == -1) && (bom[1] == -2) && (bom[2] == 0) && (bom[3] == 0)) {
      return true;
    }
    return false;
  }
}
