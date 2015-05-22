/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package backtype.storm.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-5-12")
public class JavaObject implements org.apache.thrift.TBase<JavaObject, JavaObject._Fields>, java.io.Serializable, Cloneable, Comparable<JavaObject> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("JavaObject");

  private static final org.apache.thrift.protocol.TField FULL_CLASS_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("full_class_name", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField ARGS_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("args_list", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new JavaObjectStandardSchemeFactory());
    schemes.put(TupleScheme.class, new JavaObjectTupleSchemeFactory());
  }

  private String full_class_name; // required
  private List<JavaObjectArg> args_list; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FULL_CLASS_NAME((short)1, "full_class_name"),
    ARGS_LIST((short)2, "args_list");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // FULL_CLASS_NAME
          return FULL_CLASS_NAME;
        case 2: // ARGS_LIST
          return ARGS_LIST;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FULL_CLASS_NAME, new org.apache.thrift.meta_data.FieldMetaData("full_class_name", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.ARGS_LIST, new org.apache.thrift.meta_data.FieldMetaData("args_list", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, JavaObjectArg.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(JavaObject.class, metaDataMap);
  }

  public JavaObject() {
  }

  public JavaObject(
    String full_class_name,
    List<JavaObjectArg> args_list)
  {
    this();
    this.full_class_name = full_class_name;
    this.args_list = args_list;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public JavaObject(JavaObject other) {
    if (other.is_set_full_class_name()) {
      this.full_class_name = other.full_class_name;
    }
    if (other.is_set_args_list()) {
      List<JavaObjectArg> __this__args_list = new ArrayList<JavaObjectArg>(other.args_list.size());
      for (JavaObjectArg other_element : other.args_list) {
        __this__args_list.add(new JavaObjectArg(other_element));
      }
      this.args_list = __this__args_list;
    }
  }

  public JavaObject deepCopy() {
    return new JavaObject(this);
  }

  @Override
  public void clear() {
    this.full_class_name = null;
    this.args_list = null;
  }

  public String get_full_class_name() {
    return this.full_class_name;
  }

  public void set_full_class_name(String full_class_name) {
    this.full_class_name = full_class_name;
  }

  public void unset_full_class_name() {
    this.full_class_name = null;
  }

  /** Returns true if field full_class_name is set (has been assigned a value) and false otherwise */
  public boolean is_set_full_class_name() {
    return this.full_class_name != null;
  }

  public void set_full_class_name_isSet(boolean value) {
    if (!value) {
      this.full_class_name = null;
    }
  }

  public int get_args_list_size() {
    return (this.args_list == null) ? 0 : this.args_list.size();
  }

  public java.util.Iterator<JavaObjectArg> get_args_list_iterator() {
    return (this.args_list == null) ? null : this.args_list.iterator();
  }

  public void add_to_args_list(JavaObjectArg elem) {
    if (this.args_list == null) {
      this.args_list = new ArrayList<JavaObjectArg>();
    }
    this.args_list.add(elem);
  }

  public List<JavaObjectArg> get_args_list() {
    return this.args_list;
  }

  public void set_args_list(List<JavaObjectArg> args_list) {
    this.args_list = args_list;
  }

  public void unset_args_list() {
    this.args_list = null;
  }

  /** Returns true if field args_list is set (has been assigned a value) and false otherwise */
  public boolean is_set_args_list() {
    return this.args_list != null;
  }

  public void set_args_list_isSet(boolean value) {
    if (!value) {
      this.args_list = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FULL_CLASS_NAME:
      if (value == null) {
        unset_full_class_name();
      } else {
        set_full_class_name((String)value);
      }
      break;

    case ARGS_LIST:
      if (value == null) {
        unset_args_list();
      } else {
        set_args_list((List<JavaObjectArg>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FULL_CLASS_NAME:
      return get_full_class_name();

    case ARGS_LIST:
      return get_args_list();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FULL_CLASS_NAME:
      return is_set_full_class_name();
    case ARGS_LIST:
      return is_set_args_list();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof JavaObject)
      return this.equals((JavaObject)that);
    return false;
  }

  public boolean equals(JavaObject that) {
    if (that == null)
      return false;

    boolean this_present_full_class_name = true && this.is_set_full_class_name();
    boolean that_present_full_class_name = true && that.is_set_full_class_name();
    if (this_present_full_class_name || that_present_full_class_name) {
      if (!(this_present_full_class_name && that_present_full_class_name))
        return false;
      if (!this.full_class_name.equals(that.full_class_name))
        return false;
    }

    boolean this_present_args_list = true && this.is_set_args_list();
    boolean that_present_args_list = true && that.is_set_args_list();
    if (this_present_args_list || that_present_args_list) {
      if (!(this_present_args_list && that_present_args_list))
        return false;
      if (!this.args_list.equals(that.args_list))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_full_class_name = true && (is_set_full_class_name());
    list.add(present_full_class_name);
    if (present_full_class_name)
      list.add(full_class_name);

    boolean present_args_list = true && (is_set_args_list());
    list.add(present_args_list);
    if (present_args_list)
      list.add(args_list);

    return list.hashCode();
  }

  @Override
  public int compareTo(JavaObject other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_full_class_name()).compareTo(other.is_set_full_class_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_full_class_name()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.full_class_name, other.full_class_name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_args_list()).compareTo(other.is_set_args_list());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_args_list()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.args_list, other.args_list);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("JavaObject(");
    boolean first = true;

    sb.append("full_class_name:");
    if (this.full_class_name == null) {
      sb.append("null");
    } else {
      sb.append(this.full_class_name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("args_list:");
    if (this.args_list == null) {
      sb.append("null");
    } else {
      sb.append(this.args_list);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_full_class_name()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'full_class_name' is unset! Struct:" + toString());
    }

    if (!is_set_args_list()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'args_list' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class JavaObjectStandardSchemeFactory implements SchemeFactory {
    public JavaObjectStandardScheme getScheme() {
      return new JavaObjectStandardScheme();
    }
  }

  private static class JavaObjectStandardScheme extends StandardScheme<JavaObject> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, JavaObject struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FULL_CLASS_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.full_class_name = iprot.readString();
              struct.set_full_class_name_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ARGS_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.args_list = new ArrayList<JavaObjectArg>(_list0.size);
                JavaObjectArg _elem1;
                for (int _i2 = 0; _i2 < _list0.size; ++_i2)
                {
                  _elem1 = new JavaObjectArg();
                  _elem1.read(iprot);
                  struct.args_list.add(_elem1);
                }
                iprot.readListEnd();
              }
              struct.set_args_list_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, JavaObject struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.full_class_name != null) {
        oprot.writeFieldBegin(FULL_CLASS_NAME_FIELD_DESC);
        oprot.writeString(struct.full_class_name);
        oprot.writeFieldEnd();
      }
      if (struct.args_list != null) {
        oprot.writeFieldBegin(ARGS_LIST_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.args_list.size()));
          for (JavaObjectArg _iter3 : struct.args_list)
          {
            _iter3.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class JavaObjectTupleSchemeFactory implements SchemeFactory {
    public JavaObjectTupleScheme getScheme() {
      return new JavaObjectTupleScheme();
    }
  }

  private static class JavaObjectTupleScheme extends TupleScheme<JavaObject> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, JavaObject struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.full_class_name);
      {
        oprot.writeI32(struct.args_list.size());
        for (JavaObjectArg _iter4 : struct.args_list)
        {
          _iter4.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, JavaObject struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.full_class_name = iprot.readString();
      struct.set_full_class_name_isSet(true);
      {
        org.apache.thrift.protocol.TList _list5 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.args_list = new ArrayList<JavaObjectArg>(_list5.size);
        JavaObjectArg _elem6;
        for (int _i7 = 0; _i7 < _list5.size; ++_i7)
        {
          _elem6 = new JavaObjectArg();
          _elem6.read(iprot);
          struct.args_list.add(_elem6);
        }
      }
      struct.set_args_list_isSet(true);
    }
  }

}

