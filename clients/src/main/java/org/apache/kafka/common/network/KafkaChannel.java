/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;


import java.io.IOException;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.utils.Utils;

public class KafkaChannel {

  private final String id;
  private final TransportLayer transportLayer;
  private final Authenticator authenticator;
  /**
   * Tracks accumulated network thread time. This is updated on the network thread.
   * The values are read and reset after each response is sent.
   */
  private long networkThreadTimeNanos;
  private final int maxReceiveSize;
  private NetworkReceive receive;
  private Send send;

  /**
   * Track connection and mute state of channels to enable outstanding requests on channels to be
   * processed after the channel is disconnected.
   */
  private boolean disconnected;
  private boolean muted;
  private ChannelState state;

  public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator,
      int maxReceiveSize) throws IOException {
    this.id = id;
    this.transportLayer = transportLayer;
    this.authenticator = authenticator;
    this.networkThreadTimeNanos = 0L;
    this.maxReceiveSize = maxReceiveSize;
    this.disconnected = false;
    this.muted = false;
    this.state = ChannelState.NOT_CONNECTED;
  }

  public void close() throws IOException {
    this.disconnected = true;
    Utils.closeAll(transportLayer, authenticator);
  }

  /**
   * Returns the principal returned by `authenticator.principal()`.
   */
  public Principal principal() throws IOException {
    return authenticator.principal();
  }

  /**
   * Does handshake of transportLayer and authentication using configured authenticator
   */
  public void prepare() throws IOException {
    if (!transportLayer.ready()) {
      transportLayer.handshake();
    }
    if (transportLayer.ready() && !authenticator.complete()) {
      authenticator.authenticate();
    }
    if (ready()) {
      state = ChannelState.READY;
    }
  }

  public void disconnect() {
    disconnected = true;
    transportLayer.disconnect();
  }

  public void state(ChannelState state) {
    this.state = state;
  }

  public ChannelState state() {
    return this.state;
  }

  public boolean finishConnect() throws IOException {
    boolean connected = transportLayer.finishConnect();
    if (connected) {
      state = ready() ? ChannelState.READY : ChannelState.AUTHENTICATE;
    }
    return connected;
  }

  public boolean isConnected() {
    return transportLayer.isConnected();
  }

  public String id() {
    return id;
  }

  public void mute() {
    if (!disconnected) {
      transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }
    muted = true;
  }

  public void unmute() {
    if (!disconnected) {
      transportLayer.addInterestOps(SelectionKey.OP_READ);
    }
    muted = false;
  }

  /**
   * Returns true if this channel has been explicitly muted using {@link KafkaChannel#mute()}
   */
  public boolean isMute() {
    return muted;
  }

  public boolean ready() {
    return transportLayer.ready() && authenticator.complete();
  }

  public boolean hasSend() {
    return send != null;
  }

  /**
   * Returns the address to which this channel's socket is connected or `null` if the socket has
   * never been connected.
   * <p>
   * If the socket was connected prior to being closed, then this method will continue to return
   * the
   * connected address after the socket is closed.
   */
  public InetAddress socketAddress() {
    return transportLayer.socketChannel().socket().getInetAddress();
  }

  public String socketDescription() {
    Socket socket = transportLayer.socketChannel().socket();
    if (socket.getInetAddress() == null) {
      return socket.getLocalAddress().toString();
    }
    return socket.getInetAddress().toString();
  }

  public void setSend(Send send) {
    if (this.send != null) {
      throw new IllegalStateException(
          "Attempt to begin a send operation with prior send operation still in progress, connection id is "
              + id);
    }
    this.send = send;
    this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
  }

  public NetworkReceive read() throws IOException {
    NetworkReceive result = null;

    if (receive == null) {
      receive = new NetworkReceive(maxReceiveSize, id);
    }

    receive(receive);
    if (receive.complete()) {
      receive.payload().rewind();
      result = receive;
      receive = null;
    }
    return result;
  }

  public Send write() throws IOException {
    Send result = null;
    if (send != null && send(send)) {
      result = send;
      send = null;
    }
    return result;
  }

  /**
   * Accumulates network thread time for this channel.
   */
  public void addNetworkThreadTimeNanos(long nanos) {
    networkThreadTimeNanos += nanos;
  }

  /**
   * Returns accumulated network thread time for this channel and resets
   * the value to zero.
   */
  public long getAndResetNetworkThreadTimeNanos() {
    long current = networkThreadTimeNanos;
    networkThreadTimeNanos = 0;
    return current;
  }

  private long receive(NetworkReceive receive) throws IOException {
    return receive.readFrom(transportLayer);
  }

  private boolean send(Send send) throws IOException {
    send.writeTo(transportLayer);
    if (send.completed()) {
      transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
    }

    return send.completed();
  }
}
