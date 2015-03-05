#!/bin/bash

# Error on anything that goes wrong.
set -e

cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

<configuration monitorInterval="60">
<properties>
    <property name="pattern">%d{yyyy-MM-dd HH:mm:ss.SSS} %c{1.} [%p] %msg%n</property>
    <property name="patternMetrics">%d %-8r %m%n</property>
</properties>
<appenders>
    <RollingFile name="A1"
                 fileName="\${sys:storm.home}/logs/\${sys:logfile.name}"
                 filePattern="\${sys:storm.home}/logs/\${sys:logfile.name}.%i">
        <PatternLayout>
            <pattern>\${pattern}</pattern>
        </PatternLayout>
        <Policies>
            <SizeBasedTriggeringPolicy size="100 MB"/> <!-- Or every 100 MB -->
            <DefaultRolloverStrategy max="9"/>
        </Policies>
    </RollingFile>
    <RollingFile name="ACCESS"
                 fileName="\${sys:storm.home}/logs/access.log"
                 filePattern="\${sys:storm.home}/logs/access.log.%i">
        <PatternLayout>
            <pattern>\${pattern}</pattern>
        </PatternLayout>
        <Policies>
            <SizeBasedTriggeringPolicy size="100 MB"/> <!-- Or every 100 MB -->
            <DefaultRolloverStrategy max="9"/>
        </Policies>
    </RollingFile>
    <RollingFile name="METRICS"
                 fileName="\${sys:storm.home}/logs/metrics.log"
                 filePattern="\${sys:storm.home}/logs/metrics.log.%i">
        <PatternLayout>
            <pattern>\${patternMetrics}</pattern>
        </PatternLayout>
        <Policies>
            <SizeBasedTriggeringPolicy size="2 MB"/> <!-- Or every 100 MB -->
            <DefaultRolloverStrategy max="9"/>
        </Policies>
    </RollingFile>
    <!-- Syslog name="syslog" host="localhost" port="514" protocol="UDP" appName="\${sys:servicename}">
    </Syslog -->
</appenders>
<loggers>

    <Logger name="backtype.storm.security.auth.authorizer" level="info">
        <AppenderRef ref="ACCESS"/>
    </Logger>
    <Logger name="backtype.storm.metric.LoggingMetricsConsumer" level="info">
        <AppenderRef ref="METRICS"/>
    </Logger>
    <root level="info"> <!-- We log everything -->
        <appender-ref ref="A1"/>
        <!-- appender-ref ref="syslog"/ -->
    </root>
</loggers>
</configuration>
XML
