 package com.bloom.source.cdc.common;
 
 
 public class DatabaseTable
 {
   String owner = null;
   String name = null;
   int columnCount = 0;
   boolean haskeyCols = false;
   
   public DatabaseTable(String ownerName, String tabName)
   {
     this.name = tabName;
     this.owner = ownerName;
     this.haskeyCols = false;
   }
   
   public String getOwner() {
     return this.owner;
   }
   
   public String getName() {
     return this.name;
   }
   
   public int getColumnCount() {
     return this.columnCount;
   }
   
   public boolean getHaskeyCols()
   {
     return this.haskeyCols;
   }
   
   public void setOwner(String owner) {
     this.owner = owner;
   }
   
   public void setName(String name) {
     this.name = name;
   }
   
   public void setColumnCount(int columnCount) {
     this.columnCount = columnCount;
   }
   
   public void setHaskeyCols(boolean haskeyCols) {
     this.haskeyCols = haskeyCols;
   }
 }

