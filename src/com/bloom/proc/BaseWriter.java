 package com.bloom.proc;
 
 import com.bloom.classloading.WALoader;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.BaseProcess;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.bloom.common.exc.AdapterException;
 import com.bloom.intf.Formatter;

import java.lang.reflect.Constructor;
 import java.lang.reflect.Field;
 import java.lang.reflect.Modifier;
 import java.util.List;
 import java.util.Map;
 
 
 
 
 
 
 
 public abstract class BaseWriter
   extends BaseProcess
 {
   private static final String FORMATTER_NAME = "handler";
   protected Field[] fields;
   protected Formatter formatter = null;
   
 
   public void onDeploy(Map<String, Object> writerProperties, Map<String, Object> formatterProperties, UUID inputStream)
     throws Exception
   {
     try
     {
       // 从输入流中获取元对象
       MetaInfo.Stream stream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByUUID(inputStream, WASecurityManager.TOKEN);
       // 从输入流中获取数据类型
       MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(stream.dataType, WASecurityManager.TOKEN);
       String typeName = dataType.name;
       formatterProperties.put("TypeName", typeName);
       Class<?> typeClass = WALoader.get().loadClass(dataType.className);
       this.fields = typeClass.getDeclaredFields();
       
 
 
 
       if (typeClass.getSimpleName().equals("WAEvent"))
       {
         Field[] waEventFields = new Field[3];
         for (Field field : this.fields) {
           if (Modifier.isPublic(field.getModifiers()))
           {
             if (field.getName().equals("metadata")) {
               waEventFields[0] = field;
             } else if (field.getName().equals("data")) {
               waEventFields[1] = field;
             } else if (field.getName().equals("before"))
               waEventFields[2] = field; }
         }
         this.fields = waEventFields;
         formatterProperties.put("EventType", "WAEvent");
 
 
       }
       else
       {
 
         Field[] typedEventFields = new Field[this.fields.length - 1];
         int i = 0;
         for (Field field : this.fields)
           if ((Modifier.isPublic(field.getModifiers())) && 
             (!"mapper".equals(field.getName()))) {
             typedEventFields[i] = field;
             i++;
           }
         this.fields = typedEventFields;
       }
       
 
 
 
 
       if (stream.partitioningFields.size() > 0) {
         int[] keyFieldIndex = new int[stream.partitioningFields.size()];
         int index = 0;
         for (int j = 0; j < stream.partitioningFields.size(); j++) {
           String keyFieldName = (String)stream.partitioningFields.get(j);
           for (int i = 0; i < this.fields.length; i++) {
             if (this.fields[i].getName().equals(keyFieldName)) {
               keyFieldIndex[index] = i;
               index++;
               break;
             }
           }
         }
         writerProperties.put("partitionFieldIndex", keyFieldIndex);
       }
     } catch (MetaDataRepositoryException|ClassNotFoundException e) {
       throw new AdapterException("Failure in Writer initialization. Problem in retrieving field information from stream's type", e);
     }
     
 
     String formatterClassName = (String)formatterProperties.get("handler");
     if (formatterClassName != null) {
       Class<?> formatterClass = Class.forName(formatterClassName);
       this.formatter = ((Formatter)formatterClass.getConstructor(new Class[] { Map.class, Field[].class }).newInstance(new Object[] { formatterProperties, this.fields }));
     }
   }
 }


