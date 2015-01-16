/*
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
 * Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package backtype.storm.generated;

import org.apache.commons.lang.builder.HashCodeBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Credentials implements org.apache.thrift.TBase<Credentials, Credentials._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Credentials");

  private static final org.apache.thrift.protocol.TField CREDS_FIELD_DESC = new org.apache.thrift.protocol.TField("creds", org.apache.thrift.protocol.TType.MAP, (short)1);

  private Map<String,String> creds; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    CREDS((short)1, "creds");

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
        case 1: // CREDS
          return CREDS;
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
    tmpMap.put(_Fields.CREDS, new org.apache.thrift.meta_data.FieldMetaData("creds", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Credentials.class, metaDataMap);
  }

  public Credentials() {
  }

  public Credentials(
    Map<String,String> creds)
  {
    this();
    this.creds = creds;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Credentials(Credentials other) {
    if (other.is_set_creds()) {
      Map<String,String> __this__creds = new HashMap<String,String>();
      for (Map.Entry<String, String> other_element : other.creds.entrySet()) {

        String other_element_key = other_element.getKey();
        String other_element_value = other_element.getValue();

        String __this__creds_copy_key = other_element_key;

        String __this__creds_copy_value = other_element_value;

        __this__creds.put(__this__creds_copy_key, __this__creds_copy_value);
      }
      this.creds = __this__creds;
    }
  }

  public Credentials deepCopy() {
    return new Credentials(this);
  }

  @Override
  public void clear() {
    this.creds = null;
  }

  public int get_creds_size() {
    return (this.creds == null) ? 0 : this.creds.size();
  }

  public void put_to_creds(String key, String val) {
    if (this.creds == null) {
      this.creds = new HashMap<String,String>();
    }
    this.creds.put(key, val);
  }

  public Map<String,String> get_creds() {
    return this.creds;
  }

  public void set_creds(Map<String,String> creds) {
    this.creds = creds;
  }

  public void unset_creds() {
    this.creds = null;
  }

  /** Returns true if field creds is set (has been assigned a value) and false otherwise */
  public boolean is_set_creds() {
    return this.creds != null;
  }

  public void set_creds_isSet(boolean value) {
    if (!value) {
      this.creds = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case CREDS:
      if (value == null) {
        unset_creds();
      } else {
        set_creds((Map<String,String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case CREDS:
      return get_creds();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case CREDS:
      return is_set_creds();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Credentials)
      return this.equals((Credentials)that);
    return false;
  }

  public boolean equals(Credentials that) {
    if (that == null)
      return false;

    boolean this_present_creds = true && this.is_set_creds();
    boolean that_present_creds = true && that.is_set_creds();
    if (this_present_creds || that_present_creds) {
      if (!(this_present_creds && that_present_creds))
        return false;
      if (!this.creds.equals(that.creds))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_creds = true && (is_set_creds());
    builder.append(present_creds);
    if (present_creds)
      builder.append(creds);

    return builder.toHashCode();
  }

  public int compareTo(Credentials other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Credentials typedOther = (Credentials)other;

    lastComparison = Boolean.valueOf(is_set_creds()).compareTo(typedOther.is_set_creds());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_creds()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.creds, typedOther.creds);
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
    org.apache.thrift.protocol.TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == org.apache.thrift.protocol.TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // CREDS
          if (field.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map163 = iprot.readMapBegin();
              this.creds = new HashMap<String,String>(2*_map163.size);
              for (int _i164 = 0; _i164 < _map163.size; ++_i164)
              {
                String _key165; // required
                String _val166; // required
                _key165 = iprot.readString();
                _val166 = iprot.readString();
                this.creds.put(_key165, _val166);
              }
              iprot.readMapEnd();
            }
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
    validate();
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.creds != null) {
      oprot.writeFieldBegin(CREDS_FIELD_DESC);
      {
        oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, this.creds.size()));
        for (Map.Entry<String, String> _iter167 : this.creds.entrySet())
        {
          oprot.writeString(_iter167.getKey());
          oprot.writeString(_iter167.getValue());
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Credentials(");
    boolean first = true;

    sb.append("creds:");
    if (this.creds == null) {
      sb.append("null");
    } else {
      sb.append(this.creds);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_creds()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'creds' is unset! Struct:" + toString());
    }

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

}

