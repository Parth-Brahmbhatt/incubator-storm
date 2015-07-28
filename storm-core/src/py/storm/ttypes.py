#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from thrift.Thrift import *

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class TopologyInitialStatus:
  ACTIVE = 1
  INACTIVE = 2

  _VALUES_TO_NAMES = {
    1: "ACTIVE",
    2: "INACTIVE",
  }

  _NAMES_TO_VALUES = {
    "ACTIVE": 1,
    "INACTIVE": 2,
  }


class JavaObjectArg:
  """
  Attributes:
   - int_arg
   - long_arg
   - string_arg
   - bool_arg
   - binary_arg
   - double_arg
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'int_arg', None, None, ), # 1
    (2, TType.I64, 'long_arg', None, None, ), # 2
    (3, TType.STRING, 'string_arg', None, None, ), # 3
    (4, TType.BOOL, 'bool_arg', None, None, ), # 4
    (5, TType.STRING, 'binary_arg', None, None, ), # 5
    (6, TType.DOUBLE, 'double_arg', None, None, ), # 6
  )

  def __hash__(self):
    return 0 + hash(self.int_arg) + hash(self.long_arg) + hash(self.string_arg) + hash(self.bool_arg) + hash(self.binary_arg) + hash(self.double_arg)

  def __init__(self, int_arg=None, long_arg=None, string_arg=None, bool_arg=None, binary_arg=None, double_arg=None,):
    self.int_arg = int_arg
    self.long_arg = long_arg
    self.string_arg = string_arg
    self.bool_arg = bool_arg
    self.binary_arg = binary_arg
    self.double_arg = double_arg

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.int_arg = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I64:
          self.long_arg = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.string_arg = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.BOOL:
          self.bool_arg = iprot.readBool();
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRING:
          self.binary_arg = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 6:
        if ftype == TType.DOUBLE:
          self.double_arg = iprot.readDouble();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('JavaObjectArg')
    if self.int_arg is not None:
      oprot.writeFieldBegin('int_arg', TType.I32, 1)
      oprot.writeI32(self.int_arg)
      oprot.writeFieldEnd()
    if self.long_arg is not None:
      oprot.writeFieldBegin('long_arg', TType.I64, 2)
      oprot.writeI64(self.long_arg)
      oprot.writeFieldEnd()
    if self.string_arg is not None:
      oprot.writeFieldBegin('string_arg', TType.STRING, 3)
      oprot.writeString(self.string_arg.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.bool_arg is not None:
      oprot.writeFieldBegin('bool_arg', TType.BOOL, 4)
      oprot.writeBool(self.bool_arg)
      oprot.writeFieldEnd()
    if self.binary_arg is not None:
      oprot.writeFieldBegin('binary_arg', TType.STRING, 5)
      oprot.writeString(self.binary_arg)
      oprot.writeFieldEnd()
    if self.double_arg is not None:
      oprot.writeFieldBegin('double_arg', TType.DOUBLE, 6)
      oprot.writeDouble(self.double_arg)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class JavaObject:
  """
  Attributes:
   - full_class_name
   - args_list
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'full_class_name', None, None, ), # 1
    (2, TType.LIST, 'args_list', (TType.STRUCT,(JavaObjectArg, JavaObjectArg.thrift_spec)), None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.full_class_name) + hash(self.args_list)

  def __init__(self, full_class_name=None, args_list=None,):
    self.full_class_name = full_class_name
    self.args_list = args_list

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.full_class_name = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.LIST:
          self.args_list = []
          (_etype3, _size0) = iprot.readListBegin()
          for _i4 in xrange(_size0):
            _elem5 = JavaObjectArg()
            _elem5.read(iprot)
            self.args_list.append(_elem5)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('JavaObject')
    if self.full_class_name is not None:
      oprot.writeFieldBegin('full_class_name', TType.STRING, 1)
      oprot.writeString(self.full_class_name.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.args_list is not None:
      oprot.writeFieldBegin('args_list', TType.LIST, 2)
      oprot.writeListBegin(TType.STRUCT, len(self.args_list))
      for iter6 in self.args_list:
        iter6.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.full_class_name is None:
      raise TProtocol.TProtocolException(message='Required field full_class_name is unset!')
    if self.args_list is None:
      raise TProtocol.TProtocolException(message='Required field args_list is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class NullStruct:

  thrift_spec = (
  )

  def __hash__(self):
    return 0

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('NullStruct')
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class GlobalStreamId:
  """
  Attributes:
   - componentId
   - streamId
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'componentId', None, None, ), # 1
    (2, TType.STRING, 'streamId', None, None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.componentId) + hash(self.streamId)

  def __init__(self, componentId=None, streamId=None,):
    self.componentId = componentId
    self.streamId = streamId

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.componentId = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.streamId = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('GlobalStreamId')
    if self.componentId is not None:
      oprot.writeFieldBegin('componentId', TType.STRING, 1)
      oprot.writeString(self.componentId.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.streamId is not None:
      oprot.writeFieldBegin('streamId', TType.STRING, 2)
      oprot.writeString(self.streamId.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.componentId is None:
      raise TProtocol.TProtocolException(message='Required field componentId is unset!')
    if self.streamId is None:
      raise TProtocol.TProtocolException(message='Required field streamId is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class Grouping:
  """
  Attributes:
   - fields
   - shuffle
   - all
   - none
   - direct
   - custom_object
   - custom_serialized
   - local_or_shuffle
  """

  thrift_spec = (
    None, # 0
    (1, TType.LIST, 'fields', (TType.STRING,None), None, ), # 1
    (2, TType.STRUCT, 'shuffle', (NullStruct, NullStruct.thrift_spec), None, ), # 2
    (3, TType.STRUCT, 'all', (NullStruct, NullStruct.thrift_spec), None, ), # 3
    (4, TType.STRUCT, 'none', (NullStruct, NullStruct.thrift_spec), None, ), # 4
    (5, TType.STRUCT, 'direct', (NullStruct, NullStruct.thrift_spec), None, ), # 5
    (6, TType.STRUCT, 'custom_object', (JavaObject, JavaObject.thrift_spec), None, ), # 6
    (7, TType.STRING, 'custom_serialized', None, None, ), # 7
    (8, TType.STRUCT, 'local_or_shuffle', (NullStruct, NullStruct.thrift_spec), None, ), # 8
  )

  def __hash__(self):
    return 0 + hash(self.fields) + hash(self.shuffle) + hash(self.all) + hash(self.none) + hash(self.direct) + hash(self.custom_object) + hash(self.custom_serialized) + hash(self.local_or_shuffle)

  def __init__(self, fields=None, shuffle=None, all=None, none=None, direct=None, custom_object=None, custom_serialized=None, local_or_shuffle=None,):
    self.fields = fields
    self.shuffle = shuffle
    self.all = all
    self.none = none
    self.direct = direct
    self.custom_object = custom_object
    self.custom_serialized = custom_serialized
    self.local_or_shuffle = local_or_shuffle

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.LIST:
          self.fields = []
          (_etype10, _size7) = iprot.readListBegin()
          for _i11 in xrange(_size7):
            _elem12 = iprot.readString().decode('utf-8')
            self.fields.append(_elem12)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.shuffle = NullStruct()
          self.shuffle.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRUCT:
          self.all = NullStruct()
          self.all.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRUCT:
          self.none = NullStruct()
          self.none.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRUCT:
          self.direct = NullStruct()
          self.direct.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 6:
        if ftype == TType.STRUCT:
          self.custom_object = JavaObject()
          self.custom_object.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 7:
        if ftype == TType.STRING:
          self.custom_serialized = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 8:
        if ftype == TType.STRUCT:
          self.local_or_shuffle = NullStruct()
          self.local_or_shuffle.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('Grouping')
    if self.fields is not None:
      oprot.writeFieldBegin('fields', TType.LIST, 1)
      oprot.writeListBegin(TType.STRING, len(self.fields))
      for iter13 in self.fields:
        oprot.writeString(iter13.encode('utf-8'))
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.shuffle is not None:
      oprot.writeFieldBegin('shuffle', TType.STRUCT, 2)
      self.shuffle.write(oprot)
      oprot.writeFieldEnd()
    if self.all is not None:
      oprot.writeFieldBegin('all', TType.STRUCT, 3)
      self.all.write(oprot)
      oprot.writeFieldEnd()
    if self.none is not None:
      oprot.writeFieldBegin('none', TType.STRUCT, 4)
      self.none.write(oprot)
      oprot.writeFieldEnd()
    if self.direct is not None:
      oprot.writeFieldBegin('direct', TType.STRUCT, 5)
      self.direct.write(oprot)
      oprot.writeFieldEnd()
    if self.custom_object is not None:
      oprot.writeFieldBegin('custom_object', TType.STRUCT, 6)
      self.custom_object.write(oprot)
      oprot.writeFieldEnd()
    if self.custom_serialized is not None:
      oprot.writeFieldBegin('custom_serialized', TType.STRING, 7)
      oprot.writeString(self.custom_serialized)
      oprot.writeFieldEnd()
    if self.local_or_shuffle is not None:
      oprot.writeFieldBegin('local_or_shuffle', TType.STRUCT, 8)
      self.local_or_shuffle.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class StreamInfo:
  """
  Attributes:
   - output_fields
   - direct
  """

  thrift_spec = (
    None, # 0
    (1, TType.LIST, 'output_fields', (TType.STRING,None), None, ), # 1
    (2, TType.BOOL, 'direct', None, None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.output_fields) + hash(self.direct)

  def __init__(self, output_fields=None, direct=None,):
    self.output_fields = output_fields
    self.direct = direct

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.LIST:
          self.output_fields = []
          (_etype17, _size14) = iprot.readListBegin()
          for _i18 in xrange(_size14):
            _elem19 = iprot.readString().decode('utf-8')
            self.output_fields.append(_elem19)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.BOOL:
          self.direct = iprot.readBool();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('StreamInfo')
    if self.output_fields is not None:
      oprot.writeFieldBegin('output_fields', TType.LIST, 1)
      oprot.writeListBegin(TType.STRING, len(self.output_fields))
      for iter20 in self.output_fields:
        oprot.writeString(iter20.encode('utf-8'))
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.direct is not None:
      oprot.writeFieldBegin('direct', TType.BOOL, 2)
      oprot.writeBool(self.direct)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.output_fields is None:
      raise TProtocol.TProtocolException(message='Required field output_fields is unset!')
    if self.direct is None:
      raise TProtocol.TProtocolException(message='Required field direct is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ShellComponent:
  """
  Attributes:
   - execution_command
   - script
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'execution_command', None, None, ), # 1
    (2, TType.STRING, 'script', None, None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.execution_command) + hash(self.script)

  def __init__(self, execution_command=None, script=None,):
    self.execution_command = execution_command
    self.script = script

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.execution_command = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.script = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ShellComponent')
    if self.execution_command is not None:
      oprot.writeFieldBegin('execution_command', TType.STRING, 1)
      oprot.writeString(self.execution_command.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.script is not None:
      oprot.writeFieldBegin('script', TType.STRING, 2)
      oprot.writeString(self.script.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ComponentObject:
  """
  Attributes:
   - serialized_java
   - shell
   - java_object
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'serialized_java', None, None, ), # 1
    (2, TType.STRUCT, 'shell', (ShellComponent, ShellComponent.thrift_spec), None, ), # 2
    (3, TType.STRUCT, 'java_object', (JavaObject, JavaObject.thrift_spec), None, ), # 3
  )

  def __hash__(self):
    return 0 + hash(self.serialized_java) + hash(self.shell) + hash(self.java_object)

  def __init__(self, serialized_java=None, shell=None, java_object=None,):
    self.serialized_java = serialized_java
    self.shell = shell
    self.java_object = java_object

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.serialized_java = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.shell = ShellComponent()
          self.shell.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRUCT:
          self.java_object = JavaObject()
          self.java_object.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ComponentObject')
    if self.serialized_java is not None:
      oprot.writeFieldBegin('serialized_java', TType.STRING, 1)
      oprot.writeString(self.serialized_java)
      oprot.writeFieldEnd()
    if self.shell is not None:
      oprot.writeFieldBegin('shell', TType.STRUCT, 2)
      self.shell.write(oprot)
      oprot.writeFieldEnd()
    if self.java_object is not None:
      oprot.writeFieldBegin('java_object', TType.STRUCT, 3)
      self.java_object.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ComponentCommon:
  """
  Attributes:
   - inputs
   - streams
   - parallelism_hint
   - json_conf
  """

  thrift_spec = (
    None, # 0
    (1, TType.MAP, 'inputs', (TType.STRUCT,(GlobalStreamId, GlobalStreamId.thrift_spec),TType.STRUCT,(Grouping, Grouping.thrift_spec)), None, ), # 1
    (2, TType.MAP, 'streams', (TType.STRING,None,TType.STRUCT,(StreamInfo, StreamInfo.thrift_spec)), None, ), # 2
    (3, TType.I32, 'parallelism_hint', None, None, ), # 3
    (4, TType.STRING, 'json_conf', None, None, ), # 4
  )

  def __hash__(self):
    return 0 + hash(self.inputs) + hash(self.streams) + hash(self.parallelism_hint) + hash(self.json_conf)

  def __init__(self, inputs=None, streams=None, parallelism_hint=None, json_conf=None,):
    self.inputs = inputs
    self.streams = streams
    self.parallelism_hint = parallelism_hint
    self.json_conf = json_conf

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.MAP:
          self.inputs = {}
          (_ktype22, _vtype23, _size21 ) = iprot.readMapBegin() 
          for _i25 in xrange(_size21):
            _key26 = GlobalStreamId()
            _key26.read(iprot)
            _val27 = Grouping()
            _val27.read(iprot)
            self.inputs[_key26] = _val27
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.MAP:
          self.streams = {}
          (_ktype29, _vtype30, _size28 ) = iprot.readMapBegin() 
          for _i32 in xrange(_size28):
            _key33 = iprot.readString().decode('utf-8')
            _val34 = StreamInfo()
            _val34.read(iprot)
            self.streams[_key33] = _val34
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.parallelism_hint = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRING:
          self.json_conf = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ComponentCommon')
    if self.inputs is not None:
      oprot.writeFieldBegin('inputs', TType.MAP, 1)
      oprot.writeMapBegin(TType.STRUCT, TType.STRUCT, len(self.inputs))
      for kiter35,viter36 in self.inputs.items():
        kiter35.write(oprot)
        viter36.write(oprot)
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.streams is not None:
      oprot.writeFieldBegin('streams', TType.MAP, 2)
      oprot.writeMapBegin(TType.STRING, TType.STRUCT, len(self.streams))
      for kiter37,viter38 in self.streams.items():
        oprot.writeString(kiter37.encode('utf-8'))
        viter38.write(oprot)
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.parallelism_hint is not None:
      oprot.writeFieldBegin('parallelism_hint', TType.I32, 3)
      oprot.writeI32(self.parallelism_hint)
      oprot.writeFieldEnd()
    if self.json_conf is not None:
      oprot.writeFieldBegin('json_conf', TType.STRING, 4)
      oprot.writeString(self.json_conf.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.inputs is None:
      raise TProtocol.TProtocolException(message='Required field inputs is unset!')
    if self.streams is None:
      raise TProtocol.TProtocolException(message='Required field streams is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class SpoutSpec:
  """
  Attributes:
   - spout_object
   - common
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'spout_object', (ComponentObject, ComponentObject.thrift_spec), None, ), # 1
    (2, TType.STRUCT, 'common', (ComponentCommon, ComponentCommon.thrift_spec), None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.spout_object) + hash(self.common)

  def __init__(self, spout_object=None, common=None,):
    self.spout_object = spout_object
    self.common = common

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.spout_object = ComponentObject()
          self.spout_object.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.common = ComponentCommon()
          self.common.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('SpoutSpec')
    if self.spout_object is not None:
      oprot.writeFieldBegin('spout_object', TType.STRUCT, 1)
      self.spout_object.write(oprot)
      oprot.writeFieldEnd()
    if self.common is not None:
      oprot.writeFieldBegin('common', TType.STRUCT, 2)
      self.common.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.spout_object is None:
      raise TProtocol.TProtocolException(message='Required field spout_object is unset!')
    if self.common is None:
      raise TProtocol.TProtocolException(message='Required field common is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class Bolt:
  """
  Attributes:
   - bolt_object
   - common
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'bolt_object', (ComponentObject, ComponentObject.thrift_spec), None, ), # 1
    (2, TType.STRUCT, 'common', (ComponentCommon, ComponentCommon.thrift_spec), None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.bolt_object) + hash(self.common)

  def __init__(self, bolt_object=None, common=None,):
    self.bolt_object = bolt_object
    self.common = common

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.bolt_object = ComponentObject()
          self.bolt_object.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.common = ComponentCommon()
          self.common.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('Bolt')
    if self.bolt_object is not None:
      oprot.writeFieldBegin('bolt_object', TType.STRUCT, 1)
      self.bolt_object.write(oprot)
      oprot.writeFieldEnd()
    if self.common is not None:
      oprot.writeFieldBegin('common', TType.STRUCT, 2)
      self.common.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.bolt_object is None:
      raise TProtocol.TProtocolException(message='Required field bolt_object is unset!')
    if self.common is None:
      raise TProtocol.TProtocolException(message='Required field common is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class StateSpoutSpec:
  """
  Attributes:
   - state_spout_object
   - common
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'state_spout_object', (ComponentObject, ComponentObject.thrift_spec), None, ), # 1
    (2, TType.STRUCT, 'common', (ComponentCommon, ComponentCommon.thrift_spec), None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.state_spout_object) + hash(self.common)

  def __init__(self, state_spout_object=None, common=None,):
    self.state_spout_object = state_spout_object
    self.common = common

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.state_spout_object = ComponentObject()
          self.state_spout_object.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.common = ComponentCommon()
          self.common.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('StateSpoutSpec')
    if self.state_spout_object is not None:
      oprot.writeFieldBegin('state_spout_object', TType.STRUCT, 1)
      self.state_spout_object.write(oprot)
      oprot.writeFieldEnd()
    if self.common is not None:
      oprot.writeFieldBegin('common', TType.STRUCT, 2)
      self.common.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.state_spout_object is None:
      raise TProtocol.TProtocolException(message='Required field state_spout_object is unset!')
    if self.common is None:
      raise TProtocol.TProtocolException(message='Required field common is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class StormTopology:
  """
  Attributes:
   - spouts
   - bolts
   - state_spouts
  """

  thrift_spec = (
    None, # 0
    (1, TType.MAP, 'spouts', (TType.STRING,None,TType.STRUCT,(SpoutSpec, SpoutSpec.thrift_spec)), None, ), # 1
    (2, TType.MAP, 'bolts', (TType.STRING,None,TType.STRUCT,(Bolt, Bolt.thrift_spec)), None, ), # 2
    (3, TType.MAP, 'state_spouts', (TType.STRING,None,TType.STRUCT,(StateSpoutSpec, StateSpoutSpec.thrift_spec)), None, ), # 3
  )

  def __hash__(self):
    return 0 + hash(self.spouts) + hash(self.bolts) + hash(self.state_spouts)

  def __init__(self, spouts=None, bolts=None, state_spouts=None,):
    self.spouts = spouts
    self.bolts = bolts
    self.state_spouts = state_spouts

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.MAP:
          self.spouts = {}
          (_ktype40, _vtype41, _size39 ) = iprot.readMapBegin() 
          for _i43 in xrange(_size39):
            _key44 = iprot.readString().decode('utf-8')
            _val45 = SpoutSpec()
            _val45.read(iprot)
            self.spouts[_key44] = _val45
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.MAP:
          self.bolts = {}
          (_ktype47, _vtype48, _size46 ) = iprot.readMapBegin() 
          for _i50 in xrange(_size46):
            _key51 = iprot.readString().decode('utf-8')
            _val52 = Bolt()
            _val52.read(iprot)
            self.bolts[_key51] = _val52
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.MAP:
          self.state_spouts = {}
          (_ktype54, _vtype55, _size53 ) = iprot.readMapBegin() 
          for _i57 in xrange(_size53):
            _key58 = iprot.readString().decode('utf-8')
            _val59 = StateSpoutSpec()
            _val59.read(iprot)
            self.state_spouts[_key58] = _val59
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('StormTopology')
    if self.spouts is not None:
      oprot.writeFieldBegin('spouts', TType.MAP, 1)
      oprot.writeMapBegin(TType.STRING, TType.STRUCT, len(self.spouts))
      for kiter60,viter61 in self.spouts.items():
        oprot.writeString(kiter60.encode('utf-8'))
        viter61.write(oprot)
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.bolts is not None:
      oprot.writeFieldBegin('bolts', TType.MAP, 2)
      oprot.writeMapBegin(TType.STRING, TType.STRUCT, len(self.bolts))
      for kiter62,viter63 in self.bolts.items():
        oprot.writeString(kiter62.encode('utf-8'))
        viter63.write(oprot)
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.state_spouts is not None:
      oprot.writeFieldBegin('state_spouts', TType.MAP, 3)
      oprot.writeMapBegin(TType.STRING, TType.STRUCT, len(self.state_spouts))
      for kiter64,viter65 in self.state_spouts.items():
        oprot.writeString(kiter64.encode('utf-8'))
        viter65.write(oprot)
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.spouts is None:
      raise TProtocol.TProtocolException(message='Required field spouts is unset!')
    if self.bolts is None:
      raise TProtocol.TProtocolException(message='Required field bolts is unset!')
    if self.state_spouts is None:
      raise TProtocol.TProtocolException(message='Required field state_spouts is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class AlreadyAliveException(Exception):
  """
  Attributes:
   - msg
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'msg', None, None, ), # 1
  )

  def __hash__(self):
    return 0 + hash(self.msg)

  def __init__(self, msg=None,):
    self.msg = msg

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.msg = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('AlreadyAliveException')
    if self.msg is not None:
      oprot.writeFieldBegin('msg', TType.STRING, 1)
      oprot.writeString(self.msg.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.msg is None:
      raise TProtocol.TProtocolException(message='Required field msg is unset!')
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class NotAliveException(Exception):
  """
  Attributes:
   - msg
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'msg', None, None, ), # 1
  )

  def __hash__(self):
    return 0 + hash(self.msg)

  def __init__(self, msg=None,):
    self.msg = msg

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.msg = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('NotAliveException')
    if self.msg is not None:
      oprot.writeFieldBegin('msg', TType.STRING, 1)
      oprot.writeString(self.msg.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.msg is None:
      raise TProtocol.TProtocolException(message='Required field msg is unset!')
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class AuthorizationException(Exception):
  """
  Attributes:
   - msg
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'msg', None, None, ), # 1
  )

  def __hash__(self):
    return 0 + hash(self.msg)

  def __init__(self, msg=None,):
    self.msg = msg

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.msg = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('AuthorizationException')
    if self.msg is not None:
      oprot.writeFieldBegin('msg', TType.STRING, 1)
      oprot.writeString(self.msg.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.msg is None:
      raise TProtocol.TProtocolException(message='Required field msg is unset!')
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class InvalidTopologyException(Exception):
  """
  Attributes:
   - msg
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'msg', None, None, ), # 1
  )

  def __hash__(self):
    return 0 + hash(self.msg)

  def __init__(self, msg=None,):
    self.msg = msg

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.msg = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('InvalidTopologyException')
    if self.msg is not None:
      oprot.writeFieldBegin('msg', TType.STRING, 1)
      oprot.writeString(self.msg.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.msg is None:
      raise TProtocol.TProtocolException(message='Required field msg is unset!')
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ThrottlingException(Exception):
  """
  Attributes:
   - msg
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'msg', None, None, ), # 1
  )

  def __hash__(self):
    return 0 + hash(self.msg)

  def __init__(self, msg=None,):
    self.msg = msg

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.msg = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ThrottlingException')
    if self.msg is not None:
      oprot.writeFieldBegin('msg', TType.STRING, 1)
      oprot.writeString(self.msg.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.msg is None:
      raise TProtocol.TProtocolException(message='Required field msg is unset!')
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class TopologySummary:
  """
  Attributes:
   - id
   - name
   - num_tasks
   - num_executors
   - num_workers
   - uptime_secs
   - status
   - sched_status
   - owner
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'id', None, None, ), # 1
    (2, TType.STRING, 'name', None, None, ), # 2
    (3, TType.I32, 'num_tasks', None, None, ), # 3
    (4, TType.I32, 'num_executors', None, None, ), # 4
    (5, TType.I32, 'num_workers', None, None, ), # 5
    (6, TType.I32, 'uptime_secs', None, None, ), # 6
    (7, TType.STRING, 'status', None, None, ), # 7
    None, # 8
    None, # 9
    None, # 10
    None, # 11
    None, # 12
    None, # 13
    None, # 14
    None, # 15
    None, # 16
    None, # 17
    None, # 18
    None, # 19
    None, # 20
    None, # 21
    None, # 22
    None, # 23
    None, # 24
    None, # 25
    None, # 26
    None, # 27
    None, # 28
    None, # 29
    None, # 30
    None, # 31
    None, # 32
    None, # 33
    None, # 34
    None, # 35
    None, # 36
    None, # 37
    None, # 38
    None, # 39
    None, # 40
    None, # 41
    None, # 42
    None, # 43
    None, # 44
    None, # 45
    None, # 46
    None, # 47
    None, # 48
    None, # 49
    None, # 50
    None, # 51
    None, # 52
    None, # 53
    None, # 54
    None, # 55
    None, # 56
    None, # 57
    None, # 58
    None, # 59
    None, # 60
    None, # 61
    None, # 62
    None, # 63
    None, # 64
    None, # 65
    None, # 66
    None, # 67
    None, # 68
    None, # 69
    None, # 70
    None, # 71
    None, # 72
    None, # 73
    None, # 74
    None, # 75
    None, # 76
    None, # 77
    None, # 78
    None, # 79
    None, # 80
    None, # 81
    None, # 82
    None, # 83
    None, # 84
    None, # 85
    None, # 86
    None, # 87
    None, # 88
    None, # 89
    None, # 90
    None, # 91
    None, # 92
    None, # 93
    None, # 94
    None, # 95
    None, # 96
    None, # 97
    None, # 98
    None, # 99
    None, # 100
    None, # 101
    None, # 102
    None, # 103
    None, # 104
    None, # 105
    None, # 106
    None, # 107
    None, # 108
    None, # 109
    None, # 110
    None, # 111
    None, # 112
    None, # 113
    None, # 114
    None, # 115
    None, # 116
    None, # 117
    None, # 118
    None, # 119
    None, # 120
    None, # 121
    None, # 122
    None, # 123
    None, # 124
    None, # 125
    None, # 126
    None, # 127
    None, # 128
    None, # 129
    None, # 130
    None, # 131
    None, # 132
    None, # 133
    None, # 134
    None, # 135
    None, # 136
    None, # 137
    None, # 138
    None, # 139
    None, # 140
    None, # 141
    None, # 142
    None, # 143
    None, # 144
    None, # 145
    None, # 146
    None, # 147
    None, # 148
    None, # 149
    None, # 150
    None, # 151
    None, # 152
    None, # 153
    None, # 154
    None, # 155
    None, # 156
    None, # 157
    None, # 158
    None, # 159
    None, # 160
    None, # 161
    None, # 162
    None, # 163
    None, # 164
    None, # 165
    None, # 166
    None, # 167
    None, # 168
    None, # 169
    None, # 170
    None, # 171
    None, # 172
    None, # 173
    None, # 174
    None, # 175
    None, # 176
    None, # 177
    None, # 178
    None, # 179
    None, # 180
    None, # 181
    None, # 182
    None, # 183
    None, # 184
    None, # 185
    None, # 186
    None, # 187
    None, # 188
    None, # 189
    None, # 190
    None, # 191
    None, # 192
    None, # 193
    None, # 194
    None, # 195
    None, # 196
    None, # 197
    None, # 198
    None, # 199
    None, # 200
    None, # 201
    None, # 202
    None, # 203
    None, # 204
    None, # 205
    None, # 206
    None, # 207
    None, # 208
    None, # 209
    None, # 210
    None, # 211
    None, # 212
    None, # 213
    None, # 214
    None, # 215
    None, # 216
    None, # 217
    None, # 218
    None, # 219
    None, # 220
    None, # 221
    None, # 222
    None, # 223
    None, # 224
    None, # 225
    None, # 226
    None, # 227
    None, # 228
    None, # 229
    None, # 230
    None, # 231
    None, # 232
    None, # 233
    None, # 234
    None, # 235
    None, # 236
    None, # 237
    None, # 238
    None, # 239
    None, # 240
    None, # 241
    None, # 242
    None, # 243
    None, # 244
    None, # 245
    None, # 246
    None, # 247
    None, # 248
    None, # 249
    None, # 250
    None, # 251
    None, # 252
    None, # 253
    None, # 254
    None, # 255
    None, # 256
    None, # 257
    None, # 258
    None, # 259
    None, # 260
    None, # 261
    None, # 262
    None, # 263
    None, # 264
    None, # 265
    None, # 266
    None, # 267
    None, # 268
    None, # 269
    None, # 270
    None, # 271
    None, # 272
    None, # 273
    None, # 274
    None, # 275
    None, # 276
    None, # 277
    None, # 278
    None, # 279
    None, # 280
    None, # 281
    None, # 282
    None, # 283
    None, # 284
    None, # 285
    None, # 286
    None, # 287
    None, # 288
    None, # 289
    None, # 290
    None, # 291
    None, # 292
    None, # 293
    None, # 294
    None, # 295
    None, # 296
    None, # 297
    None, # 298
    None, # 299
    None, # 300
    None, # 301
    None, # 302
    None, # 303
    None, # 304
    None, # 305
    None, # 306
    None, # 307
    None, # 308
    None, # 309
    None, # 310
    None, # 311
    None, # 312
    None, # 313
    None, # 314
    None, # 315
    None, # 316
    None, # 317
    None, # 318
    None, # 319
    None, # 320
    None, # 321
    None, # 322
    None, # 323
    None, # 324
    None, # 325
    None, # 326
    None, # 327
    None, # 328
    None, # 329
    None, # 330
    None, # 331
    None, # 332
    None, # 333
    None, # 334
    None, # 335
    None, # 336
    None, # 337
    None, # 338
    None, # 339
    None, # 340
    None, # 341
    None, # 342
    None, # 343
    None, # 344
    None, # 345
    None, # 346
    None, # 347
    None, # 348
    None, # 349
    None, # 350
    None, # 351
    None, # 352
    None, # 353
    None, # 354
    None, # 355
    None, # 356
    None, # 357
    None, # 358
    None, # 359
    None, # 360
    None, # 361
    None, # 362
    None, # 363
    None, # 364
    None, # 365
    None, # 366
    None, # 367
    None, # 368
    None, # 369
    None, # 370
    None, # 371
    None, # 372
    None, # 373
    None, # 374
    None, # 375
    None, # 376
    None, # 377
    None, # 378
    None, # 379
    None, # 380
    None, # 381
    None, # 382
    None, # 383
    None, # 384
    None, # 385
    None, # 386
    None, # 387
    None, # 388
    None, # 389
    None, # 390
    None, # 391
    None, # 392
    None, # 393
    None, # 394
    None, # 395
    None, # 396
    None, # 397
    None, # 398
    None, # 399
    None, # 400
    None, # 401
    None, # 402
    None, # 403
    None, # 404
    None, # 405
    None, # 406
    None, # 407
    None, # 408
    None, # 409
    None, # 410
    None, # 411
    None, # 412
    None, # 413
    None, # 414
    None, # 415
    None, # 416
    None, # 417
    None, # 418
    None, # 419
    None, # 420
    None, # 421
    None, # 422
    None, # 423
    None, # 424
    None, # 425
    None, # 426
    None, # 427
    None, # 428
    None, # 429
    None, # 430
    None, # 431
    None, # 432
    None, # 433
    None, # 434
    None, # 435
    None, # 436
    None, # 437
    None, # 438
    None, # 439
    None, # 440
    None, # 441
    None, # 442
    None, # 443
    None, # 444
    None, # 445
    None, # 446
    None, # 447
    None, # 448
    None, # 449
    None, # 450
    None, # 451
    None, # 452
    None, # 453
    None, # 454
    None, # 455
    None, # 456
    None, # 457
    None, # 458
    None, # 459
    None, # 460
    None, # 461
    None, # 462
    None, # 463
    None, # 464
    None, # 465
    None, # 466
    None, # 467
    None, # 468
    None, # 469
    None, # 470
    None, # 471
    None, # 472
    None, # 473
    None, # 474
    None, # 475
    None, # 476
    None, # 477
    None, # 478
    None, # 479
    None, # 480
    None, # 481
    None, # 482
    None, # 483
    None, # 484
    None, # 485
    None, # 486
    None, # 487
    None, # 488
    None, # 489
    None, # 490
    None, # 491
    None, # 492
    None, # 493
    None, # 494
    None, # 495
    None, # 496
    None, # 497
    None, # 498
    None, # 499
    None, # 500
    None, # 501
    None, # 502
    None, # 503
    None, # 504
    None, # 505
    None, # 506
    None, # 507
    None, # 508
    None, # 509
    None, # 510
    None, # 511
    None, # 512
    (513, TType.STRING, 'sched_status', None, None, ), # 513
    (514, TType.STRING, 'owner', None, None, ), # 514
  )

  def __hash__(self):
    return 0 + hash(self.id) + hash(self.name) + hash(self.num_tasks) + hash(self.num_executors) + hash(self.num_workers) + hash(self.uptime_secs) + hash(self.status) + hash(self.sched_status) + hash(self.owner)

  def __init__(self, id=None, name=None, num_tasks=None, num_executors=None, num_workers=None, uptime_secs=None, status=None, sched_status=None, owner=None,):
    self.id = id
    self.name = name
    self.num_tasks = num_tasks
    self.num_executors = num_executors
    self.num_workers = num_workers
    self.uptime_secs = uptime_secs
    self.status = status
    self.sched_status = sched_status
    self.owner = owner

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.id = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.name = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.num_tasks = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.I32:
          self.num_executors = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.I32:
          self.num_workers = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 6:
        if ftype == TType.I32:
          self.uptime_secs = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 7:
        if ftype == TType.STRING:
          self.status = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 513:
        if ftype == TType.STRING:
          self.sched_status = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 514:
        if ftype == TType.STRING:
          self.owner = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('TopologySummary')
    if self.id is not None:
      oprot.writeFieldBegin('id', TType.STRING, 1)
      oprot.writeString(self.id.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.name is not None:
      oprot.writeFieldBegin('name', TType.STRING, 2)
      oprot.writeString(self.name.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.num_tasks is not None:
      oprot.writeFieldBegin('num_tasks', TType.I32, 3)
      oprot.writeI32(self.num_tasks)
      oprot.writeFieldEnd()
    if self.num_executors is not None:
      oprot.writeFieldBegin('num_executors', TType.I32, 4)
      oprot.writeI32(self.num_executors)
      oprot.writeFieldEnd()
    if self.num_workers is not None:
      oprot.writeFieldBegin('num_workers', TType.I32, 5)
      oprot.writeI32(self.num_workers)
      oprot.writeFieldEnd()
    if self.uptime_secs is not None:
      oprot.writeFieldBegin('uptime_secs', TType.I32, 6)
      oprot.writeI32(self.uptime_secs)
      oprot.writeFieldEnd()
    if self.status is not None:
      oprot.writeFieldBegin('status', TType.STRING, 7)
      oprot.writeString(self.status.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.sched_status is not None:
      oprot.writeFieldBegin('sched_status', TType.STRING, 513)
      oprot.writeString(self.sched_status.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.owner is not None:
      oprot.writeFieldBegin('owner', TType.STRING, 514)
      oprot.writeString(self.owner.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.id is None:
      raise TProtocol.TProtocolException(message='Required field id is unset!')
    if self.name is None:
      raise TProtocol.TProtocolException(message='Required field name is unset!')
    if self.num_tasks is None:
      raise TProtocol.TProtocolException(message='Required field num_tasks is unset!')
    if self.num_executors is None:
      raise TProtocol.TProtocolException(message='Required field num_executors is unset!')
    if self.num_workers is None:
      raise TProtocol.TProtocolException(message='Required field num_workers is unset!')
    if self.uptime_secs is None:
      raise TProtocol.TProtocolException(message='Required field uptime_secs is unset!')
    if self.status is None:
      raise TProtocol.TProtocolException(message='Required field status is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class SupervisorSummary:
  """
  Attributes:
   - host
   - uptime_secs
   - num_workers
   - num_used_workers
   - supervisor_id
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'host', None, None, ), # 1
    (2, TType.I32, 'uptime_secs', None, None, ), # 2
    (3, TType.I32, 'num_workers', None, None, ), # 3
    (4, TType.I32, 'num_used_workers', None, None, ), # 4
    (5, TType.STRING, 'supervisor_id', None, None, ), # 5
  )

  def __hash__(self):
    return 0 + hash(self.host) + hash(self.uptime_secs) + hash(self.num_workers) + hash(self.num_used_workers) + hash(self.supervisor_id)

  def __init__(self, host=None, uptime_secs=None, num_workers=None, num_used_workers=None, supervisor_id=None,):
    self.host = host
    self.uptime_secs = uptime_secs
    self.num_workers = num_workers
    self.num_used_workers = num_used_workers
    self.supervisor_id = supervisor_id

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.host = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.uptime_secs = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.num_workers = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.I32:
          self.num_used_workers = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRING:
          self.supervisor_id = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('SupervisorSummary')
    if self.host is not None:
      oprot.writeFieldBegin('host', TType.STRING, 1)
      oprot.writeString(self.host.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.uptime_secs is not None:
      oprot.writeFieldBegin('uptime_secs', TType.I32, 2)
      oprot.writeI32(self.uptime_secs)
      oprot.writeFieldEnd()
    if self.num_workers is not None:
      oprot.writeFieldBegin('num_workers', TType.I32, 3)
      oprot.writeI32(self.num_workers)
      oprot.writeFieldEnd()
    if self.num_used_workers is not None:
      oprot.writeFieldBegin('num_used_workers', TType.I32, 4)
      oprot.writeI32(self.num_used_workers)
      oprot.writeFieldEnd()
    if self.supervisor_id is not None:
      oprot.writeFieldBegin('supervisor_id', TType.STRING, 5)
      oprot.writeString(self.supervisor_id.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.host is None:
      raise TProtocol.TProtocolException(message='Required field host is unset!')
    if self.uptime_secs is None:
      raise TProtocol.TProtocolException(message='Required field uptime_secs is unset!')
    if self.num_workers is None:
      raise TProtocol.TProtocolException(message='Required field num_workers is unset!')
    if self.num_used_workers is None:
      raise TProtocol.TProtocolException(message='Required field num_used_workers is unset!')
    if self.supervisor_id is None:
      raise TProtocol.TProtocolException(message='Required field supervisor_id is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ClusterSummary:
  """
  Attributes:
   - supervisors
   - nimbus_uptime_secs
   - topologies
  """

  thrift_spec = (
    None, # 0
    (1, TType.LIST, 'supervisors', (TType.STRUCT,(SupervisorSummary, SupervisorSummary.thrift_spec)), None, ), # 1
    (2, TType.I32, 'nimbus_uptime_secs', None, None, ), # 2
    (3, TType.LIST, 'topologies', (TType.STRUCT,(TopologySummary, TopologySummary.thrift_spec)), None, ), # 3
  )

  def __hash__(self):
    return 0 + hash(self.supervisors) + hash(self.nimbus_uptime_secs) + hash(self.topologies)

  def __init__(self, supervisors=None, nimbus_uptime_secs=None, topologies=None,):
    self.supervisors = supervisors
    self.nimbus_uptime_secs = nimbus_uptime_secs
    self.topologies = topologies

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.LIST:
          self.supervisors = []
          (_etype69, _size66) = iprot.readListBegin()
          for _i70 in xrange(_size66):
            _elem71 = SupervisorSummary()
            _elem71.read(iprot)
            self.supervisors.append(_elem71)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.nimbus_uptime_secs = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.LIST:
          self.topologies = []
          (_etype75, _size72) = iprot.readListBegin()
          for _i76 in xrange(_size72):
            _elem77 = TopologySummary()
            _elem77.read(iprot)
            self.topologies.append(_elem77)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ClusterSummary')
    if self.supervisors is not None:
      oprot.writeFieldBegin('supervisors', TType.LIST, 1)
      oprot.writeListBegin(TType.STRUCT, len(self.supervisors))
      for iter78 in self.supervisors:
        iter78.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.nimbus_uptime_secs is not None:
      oprot.writeFieldBegin('nimbus_uptime_secs', TType.I32, 2)
      oprot.writeI32(self.nimbus_uptime_secs)
      oprot.writeFieldEnd()
    if self.topologies is not None:
      oprot.writeFieldBegin('topologies', TType.LIST, 3)
      oprot.writeListBegin(TType.STRUCT, len(self.topologies))
      for iter79 in self.topologies:
        iter79.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.supervisors is None:
      raise TProtocol.TProtocolException(message='Required field supervisors is unset!')
    if self.nimbus_uptime_secs is None:
      raise TProtocol.TProtocolException(message='Required field nimbus_uptime_secs is unset!')
    if self.topologies is None:
      raise TProtocol.TProtocolException(message='Required field topologies is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ErrorInfo:
  """
  Attributes:
   - error
   - error_time_secs
   - host
   - port
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'error', None, None, ), # 1
    (2, TType.I32, 'error_time_secs', None, None, ), # 2
    (3, TType.STRING, 'host', None, None, ), # 3
    (4, TType.I32, 'port', None, None, ), # 4
  )

  def __hash__(self):
    return 0 + hash(self.error) + hash(self.error_time_secs) + hash(self.host) + hash(self.port)

  def __init__(self, error=None, error_time_secs=None, host=None, port=None,):
    self.error = error
    self.error_time_secs = error_time_secs
    self.host = host
    self.port = port

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.error = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.error_time_secs = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.host = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.I32:
          self.port = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ErrorInfo')
    if self.error is not None:
      oprot.writeFieldBegin('error', TType.STRING, 1)
      oprot.writeString(self.error.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.error_time_secs is not None:
      oprot.writeFieldBegin('error_time_secs', TType.I32, 2)
      oprot.writeI32(self.error_time_secs)
      oprot.writeFieldEnd()
    if self.host is not None:
      oprot.writeFieldBegin('host', TType.STRING, 3)
      oprot.writeString(self.host.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.port is not None:
      oprot.writeFieldBegin('port', TType.I32, 4)
      oprot.writeI32(self.port)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.error is None:
      raise TProtocol.TProtocolException(message='Required field error is unset!')
    if self.error_time_secs is None:
      raise TProtocol.TProtocolException(message='Required field error_time_secs is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class BoltStats:
  """
  Attributes:
   - acked
   - failed
   - process_ms_avg
   - executed
   - execute_ms_avg
  """

  thrift_spec = (
    None, # 0
    (1, TType.MAP, 'acked', (TType.STRING,None,TType.MAP,(TType.STRUCT,(GlobalStreamId, GlobalStreamId.thrift_spec),TType.I64,None)), None, ), # 1
    (2, TType.MAP, 'failed', (TType.STRING,None,TType.MAP,(TType.STRUCT,(GlobalStreamId, GlobalStreamId.thrift_spec),TType.I64,None)), None, ), # 2
    (3, TType.MAP, 'process_ms_avg', (TType.STRING,None,TType.MAP,(TType.STRUCT,(GlobalStreamId, GlobalStreamId.thrift_spec),TType.DOUBLE,None)), None, ), # 3
    (4, TType.MAP, 'executed', (TType.STRING,None,TType.MAP,(TType.STRUCT,(GlobalStreamId, GlobalStreamId.thrift_spec),TType.I64,None)), None, ), # 4
    (5, TType.MAP, 'execute_ms_avg', (TType.STRING,None,TType.MAP,(TType.STRUCT,(GlobalStreamId, GlobalStreamId.thrift_spec),TType.DOUBLE,None)), None, ), # 5
  )

  def __hash__(self):
    return 0 + hash(self.acked) + hash(self.failed) + hash(self.process_ms_avg) + hash(self.executed) + hash(self.execute_ms_avg)

  def __init__(self, acked=None, failed=None, process_ms_avg=None, executed=None, execute_ms_avg=None,):
    self.acked = acked
    self.failed = failed
    self.process_ms_avg = process_ms_avg
    self.executed = executed
    self.execute_ms_avg = execute_ms_avg

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.MAP:
          self.acked = {}
          (_ktype81, _vtype82, _size80 ) = iprot.readMapBegin() 
          for _i84 in xrange(_size80):
            _key85 = iprot.readString().decode('utf-8')
            _val86 = {}
            (_ktype88, _vtype89, _size87 ) = iprot.readMapBegin() 
            for _i91 in xrange(_size87):
              _key92 = GlobalStreamId()
              _key92.read(iprot)
              _val93 = iprot.readI64();
              _val86[_key92] = _val93
            iprot.readMapEnd()
            self.acked[_key85] = _val86
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.MAP:
          self.failed = {}
          (_ktype95, _vtype96, _size94 ) = iprot.readMapBegin() 
          for _i98 in xrange(_size94):
            _key99 = iprot.readString().decode('utf-8')
            _val100 = {}
            (_ktype102, _vtype103, _size101 ) = iprot.readMapBegin() 
            for _i105 in xrange(_size101):
              _key106 = GlobalStreamId()
              _key106.read(iprot)
              _val107 = iprot.readI64();
              _val100[_key106] = _val107
            iprot.readMapEnd()
            self.failed[_key99] = _val100
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.MAP:
          self.process_ms_avg = {}
          (_ktype109, _vtype110, _size108 ) = iprot.readMapBegin() 
          for _i112 in xrange(_size108):
            _key113 = iprot.readString().decode('utf-8')
            _val114 = {}
            (_ktype116, _vtype117, _size115 ) = iprot.readMapBegin() 
            for _i119 in xrange(_size115):
              _key120 = GlobalStreamId()
              _key120.read(iprot)
              _val121 = iprot.readDouble();
              _val114[_key120] = _val121
            iprot.readMapEnd()
            self.process_ms_avg[_key113] = _val114
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.MAP:
          self.executed = {}
          (_ktype123, _vtype124, _size122 ) = iprot.readMapBegin() 
          for _i126 in xrange(_size122):
            _key127 = iprot.readString().decode('utf-8')
            _val128 = {}
            (_ktype130, _vtype131, _size129 ) = iprot.readMapBegin() 
            for _i133 in xrange(_size129):
              _key134 = GlobalStreamId()
              _key134.read(iprot)
              _val135 = iprot.readI64();
              _val128[_key134] = _val135
            iprot.readMapEnd()
            self.executed[_key127] = _val128
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.MAP:
          self.execute_ms_avg = {}
          (_ktype137, _vtype138, _size136 ) = iprot.readMapBegin() 
          for _i140 in xrange(_size136):
            _key141 = iprot.readString().decode('utf-8')
            _val142 = {}
            (_ktype144, _vtype145, _size143 ) = iprot.readMapBegin() 
            for _i147 in xrange(_size143):
              _key148 = GlobalStreamId()
              _key148.read(iprot)
              _val149 = iprot.readDouble();
              _val142[_key148] = _val149
            iprot.readMapEnd()
            self.execute_ms_avg[_key141] = _val142
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('BoltStats')
    if self.acked is not None:
      oprot.writeFieldBegin('acked', TType.MAP, 1)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.acked))
      for kiter150,viter151 in self.acked.items():
        oprot.writeString(kiter150.encode('utf-8'))
        oprot.writeMapBegin(TType.STRUCT, TType.I64, len(viter151))
        for kiter152,viter153 in viter151.items():
          kiter152.write(oprot)
          oprot.writeI64(viter153)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.failed is not None:
      oprot.writeFieldBegin('failed', TType.MAP, 2)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.failed))
      for kiter154,viter155 in self.failed.items():
        oprot.writeString(kiter154.encode('utf-8'))
        oprot.writeMapBegin(TType.STRUCT, TType.I64, len(viter155))
        for kiter156,viter157 in viter155.items():
          kiter156.write(oprot)
          oprot.writeI64(viter157)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.process_ms_avg is not None:
      oprot.writeFieldBegin('process_ms_avg', TType.MAP, 3)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.process_ms_avg))
      for kiter158,viter159 in self.process_ms_avg.items():
        oprot.writeString(kiter158.encode('utf-8'))
        oprot.writeMapBegin(TType.STRUCT, TType.DOUBLE, len(viter159))
        for kiter160,viter161 in viter159.items():
          kiter160.write(oprot)
          oprot.writeDouble(viter161)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.executed is not None:
      oprot.writeFieldBegin('executed', TType.MAP, 4)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.executed))
      for kiter162,viter163 in self.executed.items():
        oprot.writeString(kiter162.encode('utf-8'))
        oprot.writeMapBegin(TType.STRUCT, TType.I64, len(viter163))
        for kiter164,viter165 in viter163.items():
          kiter164.write(oprot)
          oprot.writeI64(viter165)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.execute_ms_avg is not None:
      oprot.writeFieldBegin('execute_ms_avg', TType.MAP, 5)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.execute_ms_avg))
      for kiter166,viter167 in self.execute_ms_avg.items():
        oprot.writeString(kiter166.encode('utf-8'))
        oprot.writeMapBegin(TType.STRUCT, TType.DOUBLE, len(viter167))
        for kiter168,viter169 in viter167.items():
          kiter168.write(oprot)
          oprot.writeDouble(viter169)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.acked is None:
      raise TProtocol.TProtocolException(message='Required field acked is unset!')
    if self.failed is None:
      raise TProtocol.TProtocolException(message='Required field failed is unset!')
    if self.process_ms_avg is None:
      raise TProtocol.TProtocolException(message='Required field process_ms_avg is unset!')
    if self.executed is None:
      raise TProtocol.TProtocolException(message='Required field executed is unset!')
    if self.execute_ms_avg is None:
      raise TProtocol.TProtocolException(message='Required field execute_ms_avg is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class SpoutStats:
  """
  Attributes:
   - acked
   - failed
   - complete_ms_avg
  """

  thrift_spec = (
    None, # 0
    (1, TType.MAP, 'acked', (TType.STRING,None,TType.MAP,(TType.STRING,None,TType.I64,None)), None, ), # 1
    (2, TType.MAP, 'failed', (TType.STRING,None,TType.MAP,(TType.STRING,None,TType.I64,None)), None, ), # 2
    (3, TType.MAP, 'complete_ms_avg', (TType.STRING,None,TType.MAP,(TType.STRING,None,TType.DOUBLE,None)), None, ), # 3
  )

  def __hash__(self):
    return 0 + hash(self.acked) + hash(self.failed) + hash(self.complete_ms_avg)

  def __init__(self, acked=None, failed=None, complete_ms_avg=None,):
    self.acked = acked
    self.failed = failed
    self.complete_ms_avg = complete_ms_avg

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.MAP:
          self.acked = {}
          (_ktype171, _vtype172, _size170 ) = iprot.readMapBegin() 
          for _i174 in xrange(_size170):
            _key175 = iprot.readString().decode('utf-8')
            _val176 = {}
            (_ktype178, _vtype179, _size177 ) = iprot.readMapBegin() 
            for _i181 in xrange(_size177):
              _key182 = iprot.readString().decode('utf-8')
              _val183 = iprot.readI64();
              _val176[_key182] = _val183
            iprot.readMapEnd()
            self.acked[_key175] = _val176
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.MAP:
          self.failed = {}
          (_ktype185, _vtype186, _size184 ) = iprot.readMapBegin() 
          for _i188 in xrange(_size184):
            _key189 = iprot.readString().decode('utf-8')
            _val190 = {}
            (_ktype192, _vtype193, _size191 ) = iprot.readMapBegin() 
            for _i195 in xrange(_size191):
              _key196 = iprot.readString().decode('utf-8')
              _val197 = iprot.readI64();
              _val190[_key196] = _val197
            iprot.readMapEnd()
            self.failed[_key189] = _val190
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.MAP:
          self.complete_ms_avg = {}
          (_ktype199, _vtype200, _size198 ) = iprot.readMapBegin() 
          for _i202 in xrange(_size198):
            _key203 = iprot.readString().decode('utf-8')
            _val204 = {}
            (_ktype206, _vtype207, _size205 ) = iprot.readMapBegin() 
            for _i209 in xrange(_size205):
              _key210 = iprot.readString().decode('utf-8')
              _val211 = iprot.readDouble();
              _val204[_key210] = _val211
            iprot.readMapEnd()
            self.complete_ms_avg[_key203] = _val204
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('SpoutStats')
    if self.acked is not None:
      oprot.writeFieldBegin('acked', TType.MAP, 1)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.acked))
      for kiter212,viter213 in self.acked.items():
        oprot.writeString(kiter212.encode('utf-8'))
        oprot.writeMapBegin(TType.STRING, TType.I64, len(viter213))
        for kiter214,viter215 in viter213.items():
          oprot.writeString(kiter214.encode('utf-8'))
          oprot.writeI64(viter215)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.failed is not None:
      oprot.writeFieldBegin('failed', TType.MAP, 2)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.failed))
      for kiter216,viter217 in self.failed.items():
        oprot.writeString(kiter216.encode('utf-8'))
        oprot.writeMapBegin(TType.STRING, TType.I64, len(viter217))
        for kiter218,viter219 in viter217.items():
          oprot.writeString(kiter218.encode('utf-8'))
          oprot.writeI64(viter219)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.complete_ms_avg is not None:
      oprot.writeFieldBegin('complete_ms_avg', TType.MAP, 3)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.complete_ms_avg))
      for kiter220,viter221 in self.complete_ms_avg.items():
        oprot.writeString(kiter220.encode('utf-8'))
        oprot.writeMapBegin(TType.STRING, TType.DOUBLE, len(viter221))
        for kiter222,viter223 in viter221.items():
          oprot.writeString(kiter222.encode('utf-8'))
          oprot.writeDouble(viter223)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.acked is None:
      raise TProtocol.TProtocolException(message='Required field acked is unset!')
    if self.failed is None:
      raise TProtocol.TProtocolException(message='Required field failed is unset!')
    if self.complete_ms_avg is None:
      raise TProtocol.TProtocolException(message='Required field complete_ms_avg is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ExecutorSpecificStats:
  """
  Attributes:
   - bolt
   - spout
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'bolt', (BoltStats, BoltStats.thrift_spec), None, ), # 1
    (2, TType.STRUCT, 'spout', (SpoutStats, SpoutStats.thrift_spec), None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.bolt) + hash(self.spout)

  def __init__(self, bolt=None, spout=None,):
    self.bolt = bolt
    self.spout = spout

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.bolt = BoltStats()
          self.bolt.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.spout = SpoutStats()
          self.spout.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ExecutorSpecificStats')
    if self.bolt is not None:
      oprot.writeFieldBegin('bolt', TType.STRUCT, 1)
      self.bolt.write(oprot)
      oprot.writeFieldEnd()
    if self.spout is not None:
      oprot.writeFieldBegin('spout', TType.STRUCT, 2)
      self.spout.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ExecutorStats:
  """
  Attributes:
   - emitted
   - transferred
   - specific
  """

  thrift_spec = (
    None, # 0
    (1, TType.MAP, 'emitted', (TType.STRING,None,TType.MAP,(TType.STRING,None,TType.I64,None)), None, ), # 1
    (2, TType.MAP, 'transferred', (TType.STRING,None,TType.MAP,(TType.STRING,None,TType.I64,None)), None, ), # 2
    (3, TType.STRUCT, 'specific', (ExecutorSpecificStats, ExecutorSpecificStats.thrift_spec), None, ), # 3
  )

  def __hash__(self):
    return 0 + hash(self.emitted) + hash(self.transferred) + hash(self.specific)

  def __init__(self, emitted=None, transferred=None, specific=None,):
    self.emitted = emitted
    self.transferred = transferred
    self.specific = specific

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.MAP:
          self.emitted = {}
          (_ktype225, _vtype226, _size224 ) = iprot.readMapBegin() 
          for _i228 in xrange(_size224):
            _key229 = iprot.readString().decode('utf-8')
            _val230 = {}
            (_ktype232, _vtype233, _size231 ) = iprot.readMapBegin() 
            for _i235 in xrange(_size231):
              _key236 = iprot.readString().decode('utf-8')
              _val237 = iprot.readI64();
              _val230[_key236] = _val237
            iprot.readMapEnd()
            self.emitted[_key229] = _val230
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.MAP:
          self.transferred = {}
          (_ktype239, _vtype240, _size238 ) = iprot.readMapBegin() 
          for _i242 in xrange(_size238):
            _key243 = iprot.readString().decode('utf-8')
            _val244 = {}
            (_ktype246, _vtype247, _size245 ) = iprot.readMapBegin() 
            for _i249 in xrange(_size245):
              _key250 = iprot.readString().decode('utf-8')
              _val251 = iprot.readI64();
              _val244[_key250] = _val251
            iprot.readMapEnd()
            self.transferred[_key243] = _val244
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRUCT:
          self.specific = ExecutorSpecificStats()
          self.specific.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ExecutorStats')
    if self.emitted is not None:
      oprot.writeFieldBegin('emitted', TType.MAP, 1)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.emitted))
      for kiter252,viter253 in self.emitted.items():
        oprot.writeString(kiter252.encode('utf-8'))
        oprot.writeMapBegin(TType.STRING, TType.I64, len(viter253))
        for kiter254,viter255 in viter253.items():
          oprot.writeString(kiter254.encode('utf-8'))
          oprot.writeI64(viter255)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.transferred is not None:
      oprot.writeFieldBegin('transferred', TType.MAP, 2)
      oprot.writeMapBegin(TType.STRING, TType.MAP, len(self.transferred))
      for kiter256,viter257 in self.transferred.items():
        oprot.writeString(kiter256.encode('utf-8'))
        oprot.writeMapBegin(TType.STRING, TType.I64, len(viter257))
        for kiter258,viter259 in viter257.items():
          oprot.writeString(kiter258.encode('utf-8'))
          oprot.writeI64(viter259)
        oprot.writeMapEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.specific is not None:
      oprot.writeFieldBegin('specific', TType.STRUCT, 3)
      self.specific.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.emitted is None:
      raise TProtocol.TProtocolException(message='Required field emitted is unset!')
    if self.transferred is None:
      raise TProtocol.TProtocolException(message='Required field transferred is unset!')
    if self.specific is None:
      raise TProtocol.TProtocolException(message='Required field specific is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ExecutorInfo:
  """
  Attributes:
   - task_start
   - task_end
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'task_start', None, None, ), # 1
    (2, TType.I32, 'task_end', None, None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.task_start) + hash(self.task_end)

  def __init__(self, task_start=None, task_end=None,):
    self.task_start = task_start
    self.task_end = task_end

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.task_start = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.task_end = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ExecutorInfo')
    if self.task_start is not None:
      oprot.writeFieldBegin('task_start', TType.I32, 1)
      oprot.writeI32(self.task_start)
      oprot.writeFieldEnd()
    if self.task_end is not None:
      oprot.writeFieldBegin('task_end', TType.I32, 2)
      oprot.writeI32(self.task_end)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.task_start is None:
      raise TProtocol.TProtocolException(message='Required field task_start is unset!')
    if self.task_end is None:
      raise TProtocol.TProtocolException(message='Required field task_end is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ExecutorSummary:
  """
  Attributes:
   - executor_info
   - component_id
   - host
   - port
   - uptime_secs
   - stats
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'executor_info', (ExecutorInfo, ExecutorInfo.thrift_spec), None, ), # 1
    (2, TType.STRING, 'component_id', None, None, ), # 2
    (3, TType.STRING, 'host', None, None, ), # 3
    (4, TType.I32, 'port', None, None, ), # 4
    (5, TType.I32, 'uptime_secs', None, None, ), # 5
    None, # 6
    (7, TType.STRUCT, 'stats', (ExecutorStats, ExecutorStats.thrift_spec), None, ), # 7
  )

  def __hash__(self):
    return 0 + hash(self.executor_info) + hash(self.component_id) + hash(self.host) + hash(self.port) + hash(self.uptime_secs) + hash(self.stats)

  def __init__(self, executor_info=None, component_id=None, host=None, port=None, uptime_secs=None, stats=None,):
    self.executor_info = executor_info
    self.component_id = component_id
    self.host = host
    self.port = port
    self.uptime_secs = uptime_secs
    self.stats = stats

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.executor_info = ExecutorInfo()
          self.executor_info.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.component_id = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.host = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.I32:
          self.port = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.I32:
          self.uptime_secs = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 7:
        if ftype == TType.STRUCT:
          self.stats = ExecutorStats()
          self.stats.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ExecutorSummary')
    if self.executor_info is not None:
      oprot.writeFieldBegin('executor_info', TType.STRUCT, 1)
      self.executor_info.write(oprot)
      oprot.writeFieldEnd()
    if self.component_id is not None:
      oprot.writeFieldBegin('component_id', TType.STRING, 2)
      oprot.writeString(self.component_id.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.host is not None:
      oprot.writeFieldBegin('host', TType.STRING, 3)
      oprot.writeString(self.host.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.port is not None:
      oprot.writeFieldBegin('port', TType.I32, 4)
      oprot.writeI32(self.port)
      oprot.writeFieldEnd()
    if self.uptime_secs is not None:
      oprot.writeFieldBegin('uptime_secs', TType.I32, 5)
      oprot.writeI32(self.uptime_secs)
      oprot.writeFieldEnd()
    if self.stats is not None:
      oprot.writeFieldBegin('stats', TType.STRUCT, 7)
      self.stats.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.executor_info is None:
      raise TProtocol.TProtocolException(message='Required field executor_info is unset!')
    if self.component_id is None:
      raise TProtocol.TProtocolException(message='Required field component_id is unset!')
    if self.host is None:
      raise TProtocol.TProtocolException(message='Required field host is unset!')
    if self.port is None:
      raise TProtocol.TProtocolException(message='Required field port is unset!')
    if self.uptime_secs is None:
      raise TProtocol.TProtocolException(message='Required field uptime_secs is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class TopologyInfo:
  """
  Attributes:
   - id
   - name
   - uptime_secs
   - executors
   - status
   - errors
   - sched_status
   - owner
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'id', None, None, ), # 1
    (2, TType.STRING, 'name', None, None, ), # 2
    (3, TType.I32, 'uptime_secs', None, None, ), # 3
    (4, TType.LIST, 'executors', (TType.STRUCT,(ExecutorSummary, ExecutorSummary.thrift_spec)), None, ), # 4
    (5, TType.STRING, 'status', None, None, ), # 5
    (6, TType.MAP, 'errors', (TType.STRING,None,TType.LIST,(TType.STRUCT,(ErrorInfo, ErrorInfo.thrift_spec))), None, ), # 6
    None, # 7
    None, # 8
    None, # 9
    None, # 10
    None, # 11
    None, # 12
    None, # 13
    None, # 14
    None, # 15
    None, # 16
    None, # 17
    None, # 18
    None, # 19
    None, # 20
    None, # 21
    None, # 22
    None, # 23
    None, # 24
    None, # 25
    None, # 26
    None, # 27
    None, # 28
    None, # 29
    None, # 30
    None, # 31
    None, # 32
    None, # 33
    None, # 34
    None, # 35
    None, # 36
    None, # 37
    None, # 38
    None, # 39
    None, # 40
    None, # 41
    None, # 42
    None, # 43
    None, # 44
    None, # 45
    None, # 46
    None, # 47
    None, # 48
    None, # 49
    None, # 50
    None, # 51
    None, # 52
    None, # 53
    None, # 54
    None, # 55
    None, # 56
    None, # 57
    None, # 58
    None, # 59
    None, # 60
    None, # 61
    None, # 62
    None, # 63
    None, # 64
    None, # 65
    None, # 66
    None, # 67
    None, # 68
    None, # 69
    None, # 70
    None, # 71
    None, # 72
    None, # 73
    None, # 74
    None, # 75
    None, # 76
    None, # 77
    None, # 78
    None, # 79
    None, # 80
    None, # 81
    None, # 82
    None, # 83
    None, # 84
    None, # 85
    None, # 86
    None, # 87
    None, # 88
    None, # 89
    None, # 90
    None, # 91
    None, # 92
    None, # 93
    None, # 94
    None, # 95
    None, # 96
    None, # 97
    None, # 98
    None, # 99
    None, # 100
    None, # 101
    None, # 102
    None, # 103
    None, # 104
    None, # 105
    None, # 106
    None, # 107
    None, # 108
    None, # 109
    None, # 110
    None, # 111
    None, # 112
    None, # 113
    None, # 114
    None, # 115
    None, # 116
    None, # 117
    None, # 118
    None, # 119
    None, # 120
    None, # 121
    None, # 122
    None, # 123
    None, # 124
    None, # 125
    None, # 126
    None, # 127
    None, # 128
    None, # 129
    None, # 130
    None, # 131
    None, # 132
    None, # 133
    None, # 134
    None, # 135
    None, # 136
    None, # 137
    None, # 138
    None, # 139
    None, # 140
    None, # 141
    None, # 142
    None, # 143
    None, # 144
    None, # 145
    None, # 146
    None, # 147
    None, # 148
    None, # 149
    None, # 150
    None, # 151
    None, # 152
    None, # 153
    None, # 154
    None, # 155
    None, # 156
    None, # 157
    None, # 158
    None, # 159
    None, # 160
    None, # 161
    None, # 162
    None, # 163
    None, # 164
    None, # 165
    None, # 166
    None, # 167
    None, # 168
    None, # 169
    None, # 170
    None, # 171
    None, # 172
    None, # 173
    None, # 174
    None, # 175
    None, # 176
    None, # 177
    None, # 178
    None, # 179
    None, # 180
    None, # 181
    None, # 182
    None, # 183
    None, # 184
    None, # 185
    None, # 186
    None, # 187
    None, # 188
    None, # 189
    None, # 190
    None, # 191
    None, # 192
    None, # 193
    None, # 194
    None, # 195
    None, # 196
    None, # 197
    None, # 198
    None, # 199
    None, # 200
    None, # 201
    None, # 202
    None, # 203
    None, # 204
    None, # 205
    None, # 206
    None, # 207
    None, # 208
    None, # 209
    None, # 210
    None, # 211
    None, # 212
    None, # 213
    None, # 214
    None, # 215
    None, # 216
    None, # 217
    None, # 218
    None, # 219
    None, # 220
    None, # 221
    None, # 222
    None, # 223
    None, # 224
    None, # 225
    None, # 226
    None, # 227
    None, # 228
    None, # 229
    None, # 230
    None, # 231
    None, # 232
    None, # 233
    None, # 234
    None, # 235
    None, # 236
    None, # 237
    None, # 238
    None, # 239
    None, # 240
    None, # 241
    None, # 242
    None, # 243
    None, # 244
    None, # 245
    None, # 246
    None, # 247
    None, # 248
    None, # 249
    None, # 250
    None, # 251
    None, # 252
    None, # 253
    None, # 254
    None, # 255
    None, # 256
    None, # 257
    None, # 258
    None, # 259
    None, # 260
    None, # 261
    None, # 262
    None, # 263
    None, # 264
    None, # 265
    None, # 266
    None, # 267
    None, # 268
    None, # 269
    None, # 270
    None, # 271
    None, # 272
    None, # 273
    None, # 274
    None, # 275
    None, # 276
    None, # 277
    None, # 278
    None, # 279
    None, # 280
    None, # 281
    None, # 282
    None, # 283
    None, # 284
    None, # 285
    None, # 286
    None, # 287
    None, # 288
    None, # 289
    None, # 290
    None, # 291
    None, # 292
    None, # 293
    None, # 294
    None, # 295
    None, # 296
    None, # 297
    None, # 298
    None, # 299
    None, # 300
    None, # 301
    None, # 302
    None, # 303
    None, # 304
    None, # 305
    None, # 306
    None, # 307
    None, # 308
    None, # 309
    None, # 310
    None, # 311
    None, # 312
    None, # 313
    None, # 314
    None, # 315
    None, # 316
    None, # 317
    None, # 318
    None, # 319
    None, # 320
    None, # 321
    None, # 322
    None, # 323
    None, # 324
    None, # 325
    None, # 326
    None, # 327
    None, # 328
    None, # 329
    None, # 330
    None, # 331
    None, # 332
    None, # 333
    None, # 334
    None, # 335
    None, # 336
    None, # 337
    None, # 338
    None, # 339
    None, # 340
    None, # 341
    None, # 342
    None, # 343
    None, # 344
    None, # 345
    None, # 346
    None, # 347
    None, # 348
    None, # 349
    None, # 350
    None, # 351
    None, # 352
    None, # 353
    None, # 354
    None, # 355
    None, # 356
    None, # 357
    None, # 358
    None, # 359
    None, # 360
    None, # 361
    None, # 362
    None, # 363
    None, # 364
    None, # 365
    None, # 366
    None, # 367
    None, # 368
    None, # 369
    None, # 370
    None, # 371
    None, # 372
    None, # 373
    None, # 374
    None, # 375
    None, # 376
    None, # 377
    None, # 378
    None, # 379
    None, # 380
    None, # 381
    None, # 382
    None, # 383
    None, # 384
    None, # 385
    None, # 386
    None, # 387
    None, # 388
    None, # 389
    None, # 390
    None, # 391
    None, # 392
    None, # 393
    None, # 394
    None, # 395
    None, # 396
    None, # 397
    None, # 398
    None, # 399
    None, # 400
    None, # 401
    None, # 402
    None, # 403
    None, # 404
    None, # 405
    None, # 406
    None, # 407
    None, # 408
    None, # 409
    None, # 410
    None, # 411
    None, # 412
    None, # 413
    None, # 414
    None, # 415
    None, # 416
    None, # 417
    None, # 418
    None, # 419
    None, # 420
    None, # 421
    None, # 422
    None, # 423
    None, # 424
    None, # 425
    None, # 426
    None, # 427
    None, # 428
    None, # 429
    None, # 430
    None, # 431
    None, # 432
    None, # 433
    None, # 434
    None, # 435
    None, # 436
    None, # 437
    None, # 438
    None, # 439
    None, # 440
    None, # 441
    None, # 442
    None, # 443
    None, # 444
    None, # 445
    None, # 446
    None, # 447
    None, # 448
    None, # 449
    None, # 450
    None, # 451
    None, # 452
    None, # 453
    None, # 454
    None, # 455
    None, # 456
    None, # 457
    None, # 458
    None, # 459
    None, # 460
    None, # 461
    None, # 462
    None, # 463
    None, # 464
    None, # 465
    None, # 466
    None, # 467
    None, # 468
    None, # 469
    None, # 470
    None, # 471
    None, # 472
    None, # 473
    None, # 474
    None, # 475
    None, # 476
    None, # 477
    None, # 478
    None, # 479
    None, # 480
    None, # 481
    None, # 482
    None, # 483
    None, # 484
    None, # 485
    None, # 486
    None, # 487
    None, # 488
    None, # 489
    None, # 490
    None, # 491
    None, # 492
    None, # 493
    None, # 494
    None, # 495
    None, # 496
    None, # 497
    None, # 498
    None, # 499
    None, # 500
    None, # 501
    None, # 502
    None, # 503
    None, # 504
    None, # 505
    None, # 506
    None, # 507
    None, # 508
    None, # 509
    None, # 510
    None, # 511
    None, # 512
    (513, TType.STRING, 'sched_status', None, None, ), # 513
    (514, TType.STRING, 'owner', None, None, ), # 514
  )

  def __hash__(self):
    return 0 + hash(self.id) + hash(self.name) + hash(self.uptime_secs) + hash(self.executors) + hash(self.status) + hash(self.errors) + hash(self.sched_status) + hash(self.owner)

  def __init__(self, id=None, name=None, uptime_secs=None, executors=None, status=None, errors=None, sched_status=None, owner=None,):
    self.id = id
    self.name = name
    self.uptime_secs = uptime_secs
    self.executors = executors
    self.status = status
    self.errors = errors
    self.sched_status = sched_status
    self.owner = owner

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.id = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.name = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.uptime_secs = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.LIST:
          self.executors = []
          (_etype263, _size260) = iprot.readListBegin()
          for _i264 in xrange(_size260):
            _elem265 = ExecutorSummary()
            _elem265.read(iprot)
            self.executors.append(_elem265)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRING:
          self.status = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 6:
        if ftype == TType.MAP:
          self.errors = {}
          (_ktype267, _vtype268, _size266 ) = iprot.readMapBegin() 
          for _i270 in xrange(_size266):
            _key271 = iprot.readString().decode('utf-8')
            _val272 = []
            (_etype276, _size273) = iprot.readListBegin()
            for _i277 in xrange(_size273):
              _elem278 = ErrorInfo()
              _elem278.read(iprot)
              _val272.append(_elem278)
            iprot.readListEnd()
            self.errors[_key271] = _val272
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 513:
        if ftype == TType.STRING:
          self.sched_status = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 514:
        if ftype == TType.STRING:
          self.owner = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('TopologyInfo')
    if self.id is not None:
      oprot.writeFieldBegin('id', TType.STRING, 1)
      oprot.writeString(self.id.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.name is not None:
      oprot.writeFieldBegin('name', TType.STRING, 2)
      oprot.writeString(self.name.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.uptime_secs is not None:
      oprot.writeFieldBegin('uptime_secs', TType.I32, 3)
      oprot.writeI32(self.uptime_secs)
      oprot.writeFieldEnd()
    if self.executors is not None:
      oprot.writeFieldBegin('executors', TType.LIST, 4)
      oprot.writeListBegin(TType.STRUCT, len(self.executors))
      for iter279 in self.executors:
        iter279.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.status is not None:
      oprot.writeFieldBegin('status', TType.STRING, 5)
      oprot.writeString(self.status.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.errors is not None:
      oprot.writeFieldBegin('errors', TType.MAP, 6)
      oprot.writeMapBegin(TType.STRING, TType.LIST, len(self.errors))
      for kiter280,viter281 in self.errors.items():
        oprot.writeString(kiter280.encode('utf-8'))
        oprot.writeListBegin(TType.STRUCT, len(viter281))
        for iter282 in viter281:
          iter282.write(oprot)
        oprot.writeListEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.sched_status is not None:
      oprot.writeFieldBegin('sched_status', TType.STRING, 513)
      oprot.writeString(self.sched_status.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.owner is not None:
      oprot.writeFieldBegin('owner', TType.STRING, 514)
      oprot.writeString(self.owner.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.id is None:
      raise TProtocol.TProtocolException(message='Required field id is unset!')
    if self.name is None:
      raise TProtocol.TProtocolException(message='Required field name is unset!')
    if self.uptime_secs is None:
      raise TProtocol.TProtocolException(message='Required field uptime_secs is unset!')
    if self.executors is None:
      raise TProtocol.TProtocolException(message='Required field executors is unset!')
    if self.status is None:
      raise TProtocol.TProtocolException(message='Required field status is unset!')
    if self.errors is None:
      raise TProtocol.TProtocolException(message='Required field errors is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class KillOptions:
  """
  Attributes:
   - wait_secs
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'wait_secs', None, None, ), # 1
  )

  def __hash__(self):
    return 0 + hash(self.wait_secs)

  def __init__(self, wait_secs=None,):
    self.wait_secs = wait_secs

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.wait_secs = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('KillOptions')
    if self.wait_secs is not None:
      oprot.writeFieldBegin('wait_secs', TType.I32, 1)
      oprot.writeI32(self.wait_secs)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class RebalanceOptions:
  """
  Attributes:
   - wait_secs
   - num_workers
   - num_executors
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'wait_secs', None, None, ), # 1
    (2, TType.I32, 'num_workers', None, None, ), # 2
    (3, TType.MAP, 'num_executors', (TType.STRING,None,TType.I32,None), None, ), # 3
  )

  def __hash__(self):
    return 0 + hash(self.wait_secs) + hash(self.num_workers) + hash(self.num_executors)

  def __init__(self, wait_secs=None, num_workers=None, num_executors=None,):
    self.wait_secs = wait_secs
    self.num_workers = num_workers
    self.num_executors = num_executors

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.wait_secs = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.num_workers = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.MAP:
          self.num_executors = {}
          (_ktype284, _vtype285, _size283 ) = iprot.readMapBegin() 
          for _i287 in xrange(_size283):
            _key288 = iprot.readString().decode('utf-8')
            _val289 = iprot.readI32();
            self.num_executors[_key288] = _val289
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('RebalanceOptions')
    if self.wait_secs is not None:
      oprot.writeFieldBegin('wait_secs', TType.I32, 1)
      oprot.writeI32(self.wait_secs)
      oprot.writeFieldEnd()
    if self.num_workers is not None:
      oprot.writeFieldBegin('num_workers', TType.I32, 2)
      oprot.writeI32(self.num_workers)
      oprot.writeFieldEnd()
    if self.num_executors is not None:
      oprot.writeFieldBegin('num_executors', TType.MAP, 3)
      oprot.writeMapBegin(TType.STRING, TType.I32, len(self.num_executors))
      for kiter290,viter291 in self.num_executors.items():
        oprot.writeString(kiter290.encode('utf-8'))
        oprot.writeI32(viter291)
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class Credentials:
  """
  Attributes:
   - creds
  """

  thrift_spec = (
    None, # 0
    (1, TType.MAP, 'creds', (TType.STRING,None,TType.STRING,None), None, ), # 1
  )

  def __hash__(self):
    return 0 + hash(self.creds)

  def __init__(self, creds=None,):
    self.creds = creds

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.MAP:
          self.creds = {}
          (_ktype293, _vtype294, _size292 ) = iprot.readMapBegin() 
          for _i296 in xrange(_size292):
            _key297 = iprot.readString().decode('utf-8')
            _val298 = iprot.readString().decode('utf-8')
            self.creds[_key297] = _val298
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('Credentials')
    if self.creds is not None:
      oprot.writeFieldBegin('creds', TType.MAP, 1)
      oprot.writeMapBegin(TType.STRING, TType.STRING, len(self.creds))
      for kiter299,viter300 in self.creds.items():
        oprot.writeString(kiter299.encode('utf-8'))
        oprot.writeString(viter300.encode('utf-8'))
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.creds is None:
      raise TProtocol.TProtocolException(message='Required field creds is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class SubmitOptions:
  """
  Attributes:
   - initial_status
   - creds
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'initial_status', None, None, ), # 1
    (2, TType.STRUCT, 'creds', (Credentials, Credentials.thrift_spec), None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.initial_status) + hash(self.creds)

  def __init__(self, initial_status=None, creds=None,):
    self.initial_status = initial_status
    self.creds = creds

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.initial_status = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.creds = Credentials()
          self.creds.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('SubmitOptions')
    if self.initial_status is not None:
      oprot.writeFieldBegin('initial_status', TType.I32, 1)
      oprot.writeI32(self.initial_status)
      oprot.writeFieldEnd()
    if self.creds is not None:
      oprot.writeFieldBegin('creds', TType.STRUCT, 2)
      self.creds.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.initial_status is None:
      raise TProtocol.TProtocolException(message='Required field initial_status is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class DRPCRequest:
  """
  Attributes:
   - func_args
   - request_id
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'func_args', None, None, ), # 1
    (2, TType.STRING, 'request_id', None, None, ), # 2
  )

  def __hash__(self):
    return 0 + hash(self.func_args) + hash(self.request_id)

  def __init__(self, func_args=None, request_id=None,):
    self.func_args = func_args
    self.request_id = request_id

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.func_args = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.request_id = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('DRPCRequest')
    if self.func_args is not None:
      oprot.writeFieldBegin('func_args', TType.STRING, 1)
      oprot.writeString(self.func_args.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.request_id is not None:
      oprot.writeFieldBegin('request_id', TType.STRING, 2)
      oprot.writeString(self.request_id.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.func_args is None:
      raise TProtocol.TProtocolException(message='Required field func_args is unset!')
    if self.request_id is None:
      raise TProtocol.TProtocolException(message='Required field request_id is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class DRPCExecutionException(Exception):
  """
  Attributes:
   - msg
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'msg', None, None, ), # 1
  )

  def __hash__(self):
    return 0 + hash(self.msg)

  def __init__(self, msg=None,):
    self.msg = msg

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.msg = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('DRPCExecutionException')
    if self.msg is not None:
      oprot.writeFieldBegin('msg', TType.STRING, 1)
      oprot.writeString(self.msg.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.msg is None:
      raise TProtocol.TProtocolException(message='Required field msg is unset!')
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
