 package com.bloom.source.classloading;
 
 import com.bloom.source.lib.intf.Parser;
import com.bloom.uuid.UUID;
import com.bloom.common.exc.AdapterException;

import java.lang.reflect.Constructor;
import java.util.Map;
 
 
 public class ParserLoader
 {
   public static String MODULE_NAME = "handler";
   
   public static Parser createParser(Map<String, Object> prop, UUID sourceUUID) throws Exception {
     return loadParser(prop, sourceUUID);
   }
   
   public static Parser loadParser(Map<String, Object> prop, UUID sourceUUID) throws Exception
   {
     String packageName = "com.bloom.proc.";
     String version = "_1_0";
     String className = (String)prop.get(MODULE_NAME);
     if (className != null) {
       try { 
    	   String fullyQualifiedName;
         if (className.contains(".")) {
           fullyQualifiedName = className;
         } else
           fullyQualifiedName = packageName + className + version;
         Class<?> parserClass = null;
         try {
           parserClass = Class.forName(fullyQualifiedName);
         } catch (Exception exp) {
           try {
             parserClass = Class.forName(className);
           } catch (Exception sExp) {
             sExp.printStackTrace();
             throw sExp;
           }
         }
         return (Parser)parserClass.getConstructor(new Class[] { Map.class, UUID.class }).newInstance(new Object[] { prop, sourceUUID });
       }
       catch (Exception e) {
         e.printStackTrace();
         throw e;
       }
     }
     throw new AdapterException("Null parser module name passed");
   }
 }

