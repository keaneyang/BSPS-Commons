package com.bloom.source.lib.reader;

class UTF16LEBom
  extends BOM
{
  public UTF16LEBom()
  {
    byte[] tmp = { -1, -2 };
    this.pattern = tmp;
  }
}
