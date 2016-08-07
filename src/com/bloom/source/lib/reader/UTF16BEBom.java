package com.bloom.source.lib.reader;

class UTF16BEBom
  extends BOM
{
  public UTF16BEBom()
  {
    byte[] tmp = { -2, -1 };
    this.pattern = tmp;
  }
}
