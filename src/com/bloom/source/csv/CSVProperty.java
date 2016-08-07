 package com.bloom.source.csv;
 
 import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;

import java.util.Map;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class CSVProperty
   extends Property
 {
   public final String TRIM_QUOTE = "trimquote";
   public final String IGNORE_EMPTY_COLUM = "IgnoreEmptyColumn";
   public final String IGNORE_EMPTY_COLUMN_CI = "ignoreemptycolumn";
   
   public boolean header;
   public char commentcharacter;
   public int columnDelimitTill;
   public boolean trimQuote;
   public boolean ignoreEmptyColums;
   Map<String, Object> map;
   
   public Map<String, Object> getMap()
   {
     return this.map;
   }
   
 
 
 
   public CSVProperty(Map<String, Object> mp)
   {
     super(mp);
     this.map = mp;
     
 
 
 
 
 
 
     if (mp.get("header") != null) {
       this.header = getBoolean(mp, "header");
       if (this.header) {
         this.headerlineno = Constant.HEADER_LINE_NO_DEFAULT;
       } else {
         this.headerlineno = 0;
         if (mp.get("headerlineno") != null) {
           this.headerlineno = getInt(mp, "headerlineno");
         }
       }
     } else {
       this.header = false;
       this.headerlineno = 0;
       if (mp.get("headerlineno") != null) {
         this.headerlineno = getInt(mp, "headerlineno");
       }
     }
     if (mp.get("wildcard") == null) {
       this.wildcard = "*.csv";
     }
     this.commentcharacter = getChar(mp, "commentcharacter");
     
 
 
 
 
     if (mp.get("archivedir") != null) {
       this.archivedir = ((String)mp.get("archivedir"));
     } else {
       this.archivedir = null;
     }
     
     if (mp.get("columndelimittill") != null) {
       this.columnDelimitTill = getInt(mp, "columndelimittill");
       if (this.columnDelimitTill < 0) {}
 
     }
     else
     {
 
       this.columnDelimitTill = -1;
     }
     
 
 
 
 
     if (mp.get("portno") != null) {
       this.portno = getInt(mp, "portno");
     } else {
       this.portno = 0;
     }
     
     getClass(); if (mp.get("trimquote") != null) {
       getClass();this.trimQuote = getBoolean(mp, "trimquote");
     } else {
       this.trimQuote = true;
     }
     
     getClass(); if (mp.get("IgnoreEmptyColumn") == null) { getClass(); if (mp.get("ignoreemptycolumn") == null) {}
     } else { getClass();this.ignoreEmptyColums = getBoolean(mp, "ignoreemptycolumn"); return;
     }
     this.ignoreEmptyColums = false;
   }
 }

