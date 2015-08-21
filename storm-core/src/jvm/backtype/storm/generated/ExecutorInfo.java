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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-8-5")
public class ExecutorInfo implements org.apache.thrift.TBase<ExecutorInfo, ExecutorInfo._Fields>, java.io.Serializable, Cloneable, Comparable<ExecutorInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ExecutorInfo");

  private static final org.apache.thrift.protocol.TField TASK_START_FIELD_DESC = new org.apache.thrift.protocol.TField("task_start", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField TASK_END_FIELD_DESC = new org.apache.thrift.protocol.TField("task_end", org.apache.thrift.protocol.TType.I32, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ExecutorInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ExecutorInfoTupleSchemeFactory());
  }

  private int task_start; // required
  private int task_end; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TASK_START((short)1, "task_start"),
    TASK_END((short)2, "task_end");

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
        case 1: // TASK_START
          return TASK_START;
        case 2: // TASK_END
          return TASK_END;
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
  private static final int __TASK_START_ISSET_ID = 0;
  private static final int __TASK_END_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TASK_START, new org.apache.thrift.meta_data.FieldMetaData("task_start", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.TASK_END, new org.apache.thrift.meta_data.FieldMetaData("task_end", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ExecutorInfo.class, metaDataMap);
  }

  public ExecutorInfo() {
  }

  public ExecutorInfo(
    int task_start,
    int task_end)
  {
    this();
    this.task_start = task_start;
    set_task_start_isSet(true);
    this.task_end = task_end;
    set_task_end_isSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ExecutorInfo(ExecutorInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    this.task_start = other.task_start;
    this.task_end = other.task_end;
  }

  public ExecutorInfo deepCopy() {
    return new ExecutorInfo(this);
  }

  @Override
  public void clear() {
    set_task_start_isSet(false);
    this.task_start = 0;
    set_task_end_isSet(false);
    this.task_end = 0;
  }

  public int get_task_start() {
    return this.task_start;
  }

  public void set_task_start(int task_start) {
    this.task_start = task_start;
    set_task_start_isSet(true);
  }

  public void unset_task_start() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TASK_START_ISSET_ID);
  }

  /** Returns true if field task_start is set (has been assigned a value) and false otherwise */
  public boolean is_set_task_start() {
    return EncodingUtils.testBit(__isset_bitfield, __TASK_START_ISSET_ID);
  }

  public void set_task_start_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TASK_START_ISSET_ID, value);
  }

  public int get_task_end() {
    return this.task_end;
  }

  public void set_task_end(int task_end) {
    this.task_end = task_end;
    set_task_end_isSet(true);
  }

  public void unset_task_end() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TASK_END_ISSET_ID);
  }

  /** Returns true if field task_end is set (has been assigned a value) and false otherwise */
  public boolean is_set_task_end() {
    return EncodingUtils.testBit(__isset_bitfield, __TASK_END_ISSET_ID);
  }

  public void set_task_end_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TASK_END_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TASK_START:
      if (value == null) {
        unset_task_start();
      } else {
        set_task_start((Integer)value);
      }
      break;

    case TASK_END:
      if (value == null) {
        unset_task_end();
      } else {
        set_task_end((Integer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TASK_START:
      return Integer.valueOf(get_task_start());

    case TASK_END:
      return Integer.valueOf(get_task_end());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TASK_START:
      return is_set_task_start();
    case TASK_END:
      return is_set_task_end();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ExecutorInfo)
      return this.equals((ExecutorInfo)that);
    return false;
  }

  public boolean equals(ExecutorInfo that) {
    if (that == null)
      return false;

    boolean this_present_task_start = true;
    boolean that_present_task_start = true;
    if (this_present_task_start || that_present_task_start) {
      if (!(this_present_task_start && that_present_task_start))
        return false;
      if (this.task_start != that.task_start)
        return false;
    }

    boolean this_present_task_end = true;
    boolean that_present_task_end = true;
    if (this_present_task_end || that_present_task_end) {
      if (!(this_present_task_end && that_present_task_end))
        return false;
      if (this.task_end != that.task_end)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_task_start = true;
    list.add(present_task_start);
    if (present_task_start)
      list.add(task_start);

    boolean present_task_end = true;
    list.add(present_task_end);
    if (present_task_end)
      list.add(task_end);

    return list.hashCode();
  }

  @Override
  public int compareTo(ExecutorInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_task_start()).compareTo(other.is_set_task_start());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_task_start()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.task_start, other.task_start);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_task_end()).compareTo(other.is_set_task_end());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_task_end()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.task_end, other.task_end);
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
    StringBuilder sb = new StringBuilder("ExecutorInfo(");
    boolean first = true;

    sb.append("task_start:");
    sb.append(this.task_start);
    first = false;
    if (!first) sb.append(", ");
    sb.append("task_end:");
    sb.append(this.task_end);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_task_start()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'task_start' is unset! Struct:" + toString());
    }

    if (!is_set_task_end()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'task_end' is unset! Struct:" + toString());
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ExecutorInfoStandardSchemeFactory implements SchemeFactory {
    public ExecutorInfoStandardScheme getScheme() {
      return new ExecutorInfoStandardScheme();
    }
  }

  private static class ExecutorInfoStandardScheme extends StandardScheme<ExecutorInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ExecutorInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TASK_START
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.task_start = iprot.readI32();
              struct.set_task_start_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TASK_END
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.task_end = iprot.readI32();
              struct.set_task_end_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ExecutorInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(TASK_START_FIELD_DESC);
      oprot.writeI32(struct.task_start);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(TASK_END_FIELD_DESC);
      oprot.writeI32(struct.task_end);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ExecutorInfoTupleSchemeFactory implements SchemeFactory {
    public ExecutorInfoTupleScheme getScheme() {
      return new ExecutorInfoTupleScheme();
    }
  }

  private static class ExecutorInfoTupleScheme extends TupleScheme<ExecutorInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ExecutorInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI32(struct.task_start);
      oprot.writeI32(struct.task_end);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ExecutorInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.task_start = iprot.readI32();
      struct.set_task_start_isSet(true);
      struct.task_end = iprot.readI32();
      struct.set_task_end_isSet(true);
    }
  }

}

