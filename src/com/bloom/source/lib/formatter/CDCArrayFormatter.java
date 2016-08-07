package com.bloom.source.lib.formatter;

import com.bloom.classloading.WALoader;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.events.WAEvent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.security.WASecurityManager;
import com.bloom.source.lib.utils.FieldModifier;
import com.bloom.uuid.UUID;

import java.lang.reflect.Field;
import java.util.HashMap;
import org.apache.log4j.Logger;

public abstract class CDCArrayFormatter
  extends FieldModifier
{
  private Logger logger = Logger.getLogger(CDCArrayFormatter.class);
  private HashMap<UUID, Field[]> typeUUIDCache = new HashMap();
  private Field field;
  
  public CDCArrayFormatter(Field field)
  {
    this.field = field;
  }
  
  public String modifyFieldValue(Object fieldValue, Object event)
    throws Exception
  {
    Object object = this.field.get(event);
    if (object != null)
    {
      WAEvent waEvent = (WAEvent)event;
      Field[] fieldsOfThisTable = null;
      if (waEvent.typeUUID != null) {
        if (this.typeUUIDCache.containsKey(waEvent.typeUUID)) {
          fieldsOfThisTable = (Field[])this.typeUUIDCache.get(waEvent.typeUUID);
        } else {
          try
          {
            MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(waEvent.typeUUID, WASecurityManager.TOKEN);
            Class<?> typeClass = WALoader.get().loadClass(dataType.className);
            fieldsOfThisTable = typeClass.getDeclaredFields();
            this.typeUUIDCache.put(waEvent.typeUUID, fieldsOfThisTable);
          }
          catch (MetaDataRepositoryException|ClassNotFoundException e)
          {
            this.logger.warn("Unable to fetch the type for table " + waEvent.metadata.get("TableName") + e);
          }
        }
      }
      return formatCDCArray(waEvent, (Object[])object, fieldsOfThisTable);
    }
    return null;
  }
  
  public abstract String formatCDCArray(WAEvent paramWAEvent, Object[] paramArrayOfObject, Field[] paramArrayOfField);
}
