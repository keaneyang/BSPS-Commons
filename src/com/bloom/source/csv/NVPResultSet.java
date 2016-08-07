 package com.bloom.source.csv;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.smlite.NVPEvent;
import com.bloom.source.smlite.RowEvent;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 public class NVPResultSet
   extends CSVResultSetLite
 {
   Reader reader;
   private int anonymousColumnCount = 0;
   public final int NAME_INDEX = 0; public final int VALUE_INDEX = 1;
   public final String KEY_NAME = "column";
   private String lastKeyName = null;
   private String pairDelimiter = null;
   
 
 
 
 
 
 
 
 
   private Map<String, Object> map;
   
 
 
 
 
 
 
 
 
 
   public NVPResultSet(Reader reader, Property prop)
     throws IOException, InterruptedException, AdapterException
   {
     super(reader, prop);
     this.reader = reader;
     this.map = new HashMap();
     this.pairDelimiter = prop.getString("pairdelimiter", " ");
   }
   
   public void onEvent(RowEvent rowEvent) {
     super.onEvent(rowEvent);
     populateNVPmap();
     this.recordBeginOffset = rowEvent.position();
     this.anonymousColumnCount = 0;
     this.lastKeyName = null;
   }
   
   public void onEvent(NVPEvent nvpEvent) {
     this.rEvent = nvpEvent;
     setColumnData(nvpEvent);
     populateNVPmap();
     this.anonymousColumnCount += 1;
   }
   
 
 
 
   private void populateNVPmap()
   {
     String name = null;String value = null;
     
     if (this.colCount == 2) {
       name = getColumnValue(0);
       value = getColumnValue(1);
     }
     else if (this.colCount == 1)
     {
 
 
 
       value = getColumnValue(0);
       if ((this.lastKeyName == null) || (this.lastKeyName.contains("column"))) {
         if (this.logger.isDebugEnabled())
           this.logger.debug("Value [" + value + "] couln't associate with key, adding as anonymous column");
         name = "column" + this.anonymousColumnCount;
       } else {
         String prevValue = (String)this.map.get(this.lastKeyName);
         name = this.lastKeyName;
         value = prevValue + this.pairDelimiter + value;
       }
     }
     if (name != null)
       this.map.put(name, value);
     this.lastKeyName = name;
     int offset = this.columnBeginOffset[this.colCount];
     this.colCount = 0;
     this.columnBeginOffset[0] = offset;
   }
   
 
 
 
 
   public Map<String, Object> getNameValueMap()
   {
     return this.map;
   }
   
 
 
   public void clearEntries()
   {
     this.map.clear();
   }
 }

