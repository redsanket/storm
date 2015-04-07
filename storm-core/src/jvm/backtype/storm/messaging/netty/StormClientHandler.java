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
package backtype.storm.messaging.netty;

import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;
import backtype.storm.serialization.KryoValuesDeserializer;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.List;
import java.io.IOException;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormClientHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormClientHandler.class);
    private Client client;
    private KryoValuesDeserializer _des;
    
    StormClientHandler(Client client, Map conf) {
        this.client = client;
        _des = new KryoValuesDeserializer(conf);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent event) {
        //register the newly established channel
        Channel channel = event.getChannel();
        client.setChannel(channel);
        LOG.info("connection established from "+channel.getLocalAddress()+" to "+channel.getRemoteAddress());
        
        //send next batch of requests if any
        try {
            client.tryDeliverMessages(false);
        } catch (Exception ex) {
            LOG.info("exception when sending messages:", ex.getMessage());
            client.reconnect();
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        //examine the response message from server
        Object message = event.getMessage();
        if (message instanceof ControlMessage) {
          ControlMessage msg = (ControlMessage)message;
          if (msg==ControlMessage.FAILURE_RESPONSE)
              LOG.info("failure response:{}", msg);

          //send next batch of requests if any
          try {
              client.tryDeliverMessages(false);
          } catch (Exception ex) {
              LOG.info("exception when sending messages:", ex.getMessage());
              client.reconnect();
          }
        } else if (message instanceof List) {
          try {
            //This should be the metrics, and there should only be one of them
            List<TaskMessage> list = (List<TaskMessage>)message;
            if (list.size() < 1) throw new RuntimeException("Didn't see enough load metrics ("+client.remote_addr+") "+list);
            if (list.size() != 1) LOG.warn("Messages are not being delivered fast enough, got "+list.size()+" metrics messages at once("+client.remote_addr+")");
            TaskMessage tm = ((List<TaskMessage>)message).get(list.size() - 1);
            if (tm.task() != -1) throw new RuntimeException("Metrics messages are sent to the system task ("+client.remote_addr+") "+tm);
            List metrics = _des.deserialize(tm.message());
            if (metrics.size() < 1) throw new RuntimeException("No metrics data in the metrics message ("+client.remote_addr+") "+metrics);
            if (!(metrics.get(0) instanceof Map)) throw new RuntimeException("The metrics did not have a map in the first slot ("+client.remote_addr+") "+metrics);
            client.setLoadMetrics((Map<Integer, Double>)metrics.get(0));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else {
          throw new RuntimeException("Don't know how to handle a message of type "+message+" ("+client.remote_addr+")");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        Throwable cause = event.getCause();
        if (!(cause instanceof ConnectException)) {
            LOG.info("Connection to "+client.remote_addr+" failed:", cause);
        }
        client.reconnect();
    }
}
