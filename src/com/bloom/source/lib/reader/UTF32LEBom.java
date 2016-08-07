package com.bloom.source.lib.reader;

class UTF32LEBom
  extends BOM
{
  public UTF32LEBom()
  {
    byte[] tmp = { -1, -2, 0, 0 };
    this.pattern = tmp;
  }
}
