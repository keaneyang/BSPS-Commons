package com.bloom.source.lib.utils;

import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

public class JSONObjectMember
{
  private List<Member> memberList;
  private Logger logger = Logger.getLogger(JSONObjectMember.class);
  private FieldModifier[] fieldModifiers;
  
  public JSONObjectMember(String memberList, Field[] fields, FieldModifier[] fieldModifiers)
  {
    this.memberList = new ArrayList();
    if (fieldModifiers != null) {
      this.fieldModifiers = ((FieldModifier[])Arrays.copyOf(fieldModifiers, fieldModifiers.length));
    }
    if (memberList != null)
    {
      String[] members = memberList.split(",");
      initializeMemberList(members, fields);
    }
    else
    {
      initializeDefaultMemberList(fields);
    }
    if (this.memberList.isEmpty()) {
      this.logger.error("Initialized member list from " + memberList + " is empty. Please check if field names " + Arrays.toString(fields) + " of type of the stream match with member names");
    } else if (this.logger.isTraceEnabled()) {
      this.logger.trace("Initialized members list is " + this.memberList.toString());
    }
  }
  
  public List<Member> getMemberList()
  {
    return Collections.unmodifiableList(this.memberList);
  }
  
  private void initializeMemberList(String[] members, Field[] fields)
  {
    for (String member : members)
    {
      int fieldIndex = 0;
      for (Field field : fields)
      {
        if (field.getName().equalsIgnoreCase(member))
        {
          Member memberInstance = new Member(member, field, fieldIndex, null);
          this.memberList.add(memberInstance);
          break;
        }
        fieldIndex++;
      }
    }
  }
  
  private void initializeDefaultMemberList(Field[] fields)
  {
    int fieldIndex = 0;
    for (Field field : fields)
    {
      Member memberInstance = new Member(field.getName(), field, fieldIndex, null);
      this.memberList.add(memberInstance);
      fieldIndex++;
    }
  }
  
  public class Member
  {
    private String name;
    private Field field;
    private int fieldIndex;
    
    public Member(String name, Field field, int fieldIndex, Field subField)
    {
      this.name = name;
      this.field = field;
      this.fieldIndex = fieldIndex;
    }
    
    public String processValue(Object event)
      throws Exception
    {
      boolean dontProcess = false;
      String value;
      try
      {
        Object object = this.field.get(event);
        if (object == null) {
          return "null";
        }
        value = JSONObjectMember.this.fieldModifiers[this.fieldIndex].modifyFieldValue(object, event);
        if ((this.field.getType().isArray()) || (Map.class.isAssignableFrom(this.field.getType()))) {
          dontProcess = true;
        }
        if (!dontProcess) {
          value = StringEscapeUtils.escapeJson(value);
        }
      }
      catch (IllegalArgumentException|IllegalAccessException e)
      {
        AdapterException se = new AdapterException(Error.GENERIC_EXCEPTION, e);
        throw se;
      }
      if ((!ClassUtils.isPrimitiveOrWrapper(this.field.getType())) && (!dontProcess)) {
        return "\"" + value + "\"";
      }
      return value;
    }
    
    public String getName()
    {
      return "\"" + this.name + "\"";
    }
    
    public String toString()
    {
      return "Member name is " + getName() + ". Corresponding field name is " + this.field.getName();
    }
  }
}
