 package com.bloom.source.json;
 
 import com.bloom.source.lib.prop.Property;

import java.util.Map;
 
 
 
 
 
 
 
 
 
 
 
 
 public class JSONProperty
   extends Property
 {
   public String type;
   Map<String, Object> map;
   
   public Map<String, Object> getMap()
   {
     return this.map;
   }
   
 
 
 
 
   public JSONProperty(Map<String, Object> mp)
   {
     super(mp);
     this.map = mp;
     
     if (mp.get("wildcard") == null) {
       this.wildcard = "*.json";
     }
     else {
       this.wildcard = ((String)mp.get("wildcard"));
     }
     
 
 
     if (mp.get("portno") != null) {
       this.portno = getInt(mp, "portno");
     } else {
       this.portno = 0;
     }
   }
 }

