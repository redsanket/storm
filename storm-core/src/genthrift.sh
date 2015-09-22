#!/usr/bin/env bash
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

cp -r `pwd`/jvm/backtype/storm/generated/ `pwd`/jvm/backtype/storm/generated-tmp/
rm -rf gen-javabean gen-py py
rm -rf jvm/backtype/storm/generated
thrift --gen java:beans,hashcode,nocamel --gen py:utf8strings storm.thrift
for file in gen-javabean/backtype/storm/generated/* ; do
  cat java_license_header.txt ${file} > ${file}.tmp
  mv -f ${file}.tmp ${file}
done
cat py_license_header.txt gen-py/__init__.py > gen-py/__init__.py.tmp
mv gen-py/__init__.py.tmp gen-py/__init__.py
for file in gen-py/storm/* ; do
  cat py_license_header.txt ${file} > ${file}.tmp
  mv -f ${file}.tmp ${file}
done
mv gen-javabean/backtype/storm/generated jvm/backtype/storm/generated
mv gen-py py
rm -rf gen-javabean

#code comparision here
for file in `pwd`/jvm/backtype/storm/generated-tmp/* ; do
    fullpath=${file}
    base=`echo $fullpath | xargs -n 1 basename`
    diff_file=`pwd`/jvm/backtype/storm/generated/$base
    diff_output=`diff $fullpath $diff_file | grep '54c54'`
    lines=`diff $fullpath $diff_file | wc -l`
    if [ "$diff_output" = '54c54' ] && [ $lines -lt 6 ]; then
        cp `pwd`/jvm/backtype/storm/generated-tmp/$base `pwd`/jvm/backtype/storm/generated/$base 
    fi
done
rm -rf `pwd`/jvm/backtype/storm/generated-tmp/
