package com.bloom.source.lib.reader;

class UTF8Bom
  extends BOM
{
  public UTF8Bom()
  {
    byte[] tmp = { -17, -69, -65 };
    this.pattern = tmp;
  }
}
