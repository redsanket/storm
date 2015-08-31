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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-8-31")
public class Credentials implements org.apache.thrift.TBase<Credentials, Credentials._Fields>, java.io.Serializable, Cloneable, Comparable<Credentials> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Credentials");

  private static final org.apache.thrift.protocol.TField CREDS_FIELD_DESC = new org.apache.thrift.protocol.TField("creds", org.apache.thrift.protocol.TType.MAP, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CredentialsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CredentialsTupleSchemeFactory());
  }

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
      Map<String,String> __this__creds = new HashMap<String,String>(other.creds);
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
    List<Object> list = new ArrayList<Object>();

    boolean present_creds = true && (is_set_creds());
    list.add(present_creds);
    if (present_creds)
      list.add(creds);

    return list.hashCode();
  }

  @Override
  public int compareTo(Credentials other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_creds()).compareTo(other.is_set_creds());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_creds()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.creds, other.creds);
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

  private static class CredentialsStandardSchemeFactory implements SchemeFactory {
    public CredentialsStandardScheme getScheme() {
      return new CredentialsStandardScheme();
    }
  }

  private static class CredentialsStandardScheme extends StandardScheme<Credentials> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Credentials struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CREDS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map452 = iprot.readMapBegin();
                struct.creds = new HashMap<String,String>(2*_map452.size);
                String _key453;
                String _val454;
                for (int _i455 = 0; _i455 < _map452.size; ++_i455)
                {
                  _key453 = iprot.readString();
                  _val454 = iprot.readString();
                  struct.creds.put(_key453, _val454);
                }
                iprot.readMapEnd();
              }
              struct.set_creds_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, Credentials struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.creds != null) {
        oprot.writeFieldBegin(CREDS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.creds.size()));
          for (Map.Entry<String, String> _iter456 : struct.creds.entrySet())
          {
            oprot.writeString(_iter456.getKey());
            oprot.writeString(_iter456.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CredentialsTupleSchemeFactory implements SchemeFactory {
    public CredentialsTupleScheme getScheme() {
      return new CredentialsTupleScheme();
    }
  }

  private static class CredentialsTupleScheme extends TupleScheme<Credentials> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Credentials struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.creds.size());
        for (Map.Entry<String, String> _iter457 : struct.creds.entrySet())
        {
          oprot.writeString(_iter457.getKey());
          oprot.writeString(_iter457.getValue());
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Credentials struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map458 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
        struct.creds = new HashMap<String,String>(2*_map458.size);
        String _key459;
        String _val460;
        for (int _i461 = 0; _i461 < _map458.size; ++_i461)
        {
          _key459 = iprot.readString();
          _val460 = iprot.readString();
          struct.creds.put(_key459, _val460);
        }
      }
      struct.set_creds_isSet(true);
    }
  }

}

