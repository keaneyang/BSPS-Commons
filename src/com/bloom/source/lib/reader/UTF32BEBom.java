package com.bloom.source.lib.reader;

class UTF32BEBom
  extends BOM
{
  public UTF32BEBom()
  {
    byte[] tmp = { 0, 0, -2, -1 };
    this.pattern = tmp;
  }
}
