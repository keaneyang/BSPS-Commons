 package com.bloom.proc;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.utils.FieldModifier;
import com.bloom.common.exc.AdapterException;
import com.bloom.intf.Formatter;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public abstract class BaseFormatter
   implements Formatter
 {
   protected Field[] fields;
   protected String charset;
   protected String rowDelimiter;
   protected FieldModifier[] fieldModifiers;
   
   public BaseFormatter() {}
   
   public BaseFormatter(Map<String, Object> formatterProperties, Field[] fields)
     throws AdapterException
   {
     if (fields != null)
       this.fields = ((Field[])Arrays.copyOf(fields, fields.length));
     Property prop = new Property(formatterProperties);
     String charset = (String)formatterProperties.get(Property.CHARSET);
     if ((charset != null) && (!charset.isEmpty())) {
       this.charset = charset;
     } else {
       this.charset = Charset.defaultCharset().name();
     }
     this.rowDelimiter = prop.getString("rowdelimiter", "\n");
   }
 }

