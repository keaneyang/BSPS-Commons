package com.bloom.source.lib.meta;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class MetaData
{
  List<Column> columns;
  
  public MetaData(Column[] cols)
  {
    init();
    for (int itr = 0; itr < cols.length; itr++) {
      this.columns.add(cols[itr]);
    }
  }
  
  public void init()
  {
    this.columns = new ArrayList();
  }
  
  public MetaData(String metaDataFile) {}
  
  public void addColumn(Column col)
  {
    this.columns.add(col);
  }
  
  public void addColumn(Column col, int index)
  {
    this.columns.add(index, col);
  }
  
  public void clear()
  {
    this.columns.clear();
  }
  
  public Column[] colums()
  {
    Column[] colList = new Column[this.columns.size()];
    for (int itr = 0; itr < this.columns.size(); itr++) {
      colList[itr] = ((Column)this.columns.get(itr));
    }
    return colList;
  }
  
  public void dump()
  {
    System.out.println("*********** Column List ***************");
    for (int itr = 0; itr < this.columns.size(); itr++) {
      System.out.println("Column [" + itr + "] : [" + ((Column)this.columns.get(itr)).toString() + "]");
    }
    System.out.println("***************************************");
  }
}
