/*******************************************************************************
 * Copyright (c) 1999, 2014 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.mohdroid.android.mqtt5.core;


import org.eclipse.paho.mqttv5.client.IMqttAsyncClient;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttActionListener;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSecurityException;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.MqttWireMessage;

/**
 * <p>
 * Implementation of the IMqttToken interface for use from within the
 * MqttAndroidClient implementation
 */

class MqttTokenAndroid implements IMqttToken {

  private MqttActionListener listener;

  private volatile boolean isComplete;

  private volatile MqttException lastException;

  private Object waitObject = new Object();

  private IMqttAsyncClient client;

  private Object userContext;

  private String[] topics;

  private IMqttToken delegate; // specifically for getMessageId

  private MqttException pendingException;

  /**
   * Standard constructor
   * 
   * @param client used to pass MqttAndroidClient object
   * @param userContext used to pass context
   * @param listener optional listener that will be notified when the action completes. Use null if not required.
   */
  MqttTokenAndroid(MqttAndroidClient client,
      Object userContext, MqttActionListener listener) {
    this(client, userContext, listener, null);
  }

  /**
   * Constructor for use with subscribe operations
   * 
   * @param client used to pass MqttAndroidClient object
   * @param userContext used to pass context
   * @param listener optional listener that will be notified when the action completes. Use null if not required.
   * @param topics topics to subscribe to, which can include wildcards.
   */
  MqttTokenAndroid(MqttAndroidClient client,
      Object userContext, MqttActionListener listener, String[] topics) {
    this.client = client;
    this.userContext = userContext;
    this.listener = listener;
    this.topics = topics;
  }

  /**
   * @see IMqttToken#waitForCompletion()
   */
  @Override
  public void waitForCompletion() throws MqttException, MqttSecurityException {
    synchronized (waitObject) {
      try {
        waitObject.wait();
      }
      catch (InterruptedException e) {
        // do nothing
      }
    }
    if (pendingException != null) {
      throw pendingException;
    }
  }

  /**
   * @see IMqttToken#waitForCompletion(long)
   */
  @Override
  public void waitForCompletion(long timeout) throws MqttException,
      MqttSecurityException {
    synchronized (waitObject) {
      try {
        waitObject.wait(timeout);
      }
      catch (InterruptedException e) {
        // do nothing
      }
      if (!isComplete) { //v3 use REASON_CODE_CLIENT_TIMEOUT?
        throw new MqttException(MqttException.REASON_CODE_CLIENT_EXCEPTION);
      }
      if (pendingException != null) {
        throw pendingException;
      }
    }
  }

  /**
   * notify successful completion of the operation
   */
  void notifyComplete() {
    synchronized (waitObject) {
      isComplete = true;
      waitObject.notifyAll();
      if (listener != null) {
        listener.onSuccess(this);
      }
    }
  }

  /**
   * notify unsuccessful completion of the operation
   */
  void notifyFailure(Throwable exception) {
    synchronized (waitObject) {
      isComplete = true;
      if (exception instanceof MqttException) {
        pendingException = (MqttException) exception;
      }
      else {
        pendingException = new MqttException(exception);
      }
      waitObject.notifyAll();
      if (exception instanceof MqttException) {
        lastException = (MqttException) exception;
      }
      if (listener != null) {
        listener.onFailure(this, exception);
      }
    }

  }

  /**
   * @see IMqttToken#isComplete()
   */
  @Override
  public boolean isComplete() {
    return isComplete;
  }

  void setComplete(boolean complete) {
    isComplete = complete;
  }

  /**
   * @see IMqttToken#getException()
   */
  @Override
  public MqttException getException() {
    return lastException;
  }

  void setException(MqttException exception) {
    lastException = exception;
  }

  /**
   * @see IMqttToken#getClient()
   */
  @Override
  public MqttAsyncClient getClient() {
    return (MqttAsyncClient) client;
  }

  /**
   * @see IMqttToken#setActionCallback(MqttActionListener)
   */
  @Override
  public void setActionCallback(MqttActionListener listener) {
    this.listener = listener;
  }

  /**
   * @see IMqttToken#getActionCallback()
   */
  @Override
  public MqttActionListener getActionCallback() {
    return listener;
  }

  /**
   * @see IMqttToken#getTopics()
   */
  @Override
  public String[] getTopics() {
    return topics;
  }

  /**
   * @see IMqttToken#setUserContext(Object)
   */
  @Override
  public void setUserContext(Object userContext) {
    this.userContext = userContext;

  }

  /**
   * @see IMqttToken#getUserContext()
   */
  @Override
  public Object getUserContext() {
    return userContext;
  }

  void setDelegate(IMqttToken delegate) {
    this.delegate = delegate;
  }

  /**
   * @see IMqttToken#getMessageId()
   */
  @Override
  public int getMessageId() {
    return (delegate != null) ? delegate.getMessageId() : 0;
  }
  
  @Override
  public MqttWireMessage getResponse() {
    return delegate.getResponse();
  }

  /**
   * @return the response wire message properties
   */
  @Override
  public MqttProperties getResponseProperties() {
    return delegate.getResponseProperties();
  }

  /**
   * Returns the message associated with this token.
   * <p>Until the message has been delivered, the message being delivered will
   * be returned. Once the message has been delivered <code>null</code> will be
   * returned.
   *
   * @return the message associated with this token or null if already delivered.
   * @throws MqttException if there was a problem completing retrieving the message
   */
  @Override
  public MqttMessage getMessage() throws MqttException {
    return delegate.getMessage();
  }

  /**
   * @return the request wire message
   */
  @Override
  public MqttWireMessage getRequestMessage() {
    return delegate.getRequestMessage();
  }

  /**
   * @return the request wire message properties
   */
  @Override
  public MqttProperties getRequestProperties() {
    return delegate.getRequestProperties();
  }

  @Override
  public boolean getSessionPresent() {
    return delegate.getSessionPresent();
  }
  
  @Override
  public int[] getGrantedQos() {
    return delegate.getGrantedQos();
  }

  /**
   * Returns a list of reason codes that were returned as a result of this token's action.
   * You will receive reason codes from the following MQTT actions:
   * <ul>
   * <li>CONNECT - in the corresponding CONNACK Packet.</li>
   * <li>PUBLISH - in the corresponding PUBACK, PUBREC, PUBCOMP, PUBREL packets</li>
   * <li>SUBSCRIBE - in the corresponding SUBACK Packet.</li>
   * <li>UNSUBSCRIBE - in the corresponding UNSUBACK Packet.</li>
   * <li>AUTH - in the returned AUTH Packet.</li>
   * </ul>
   *
   * @return the reason code(s) from the response for this token's action.
   */
  @Override
  public int[] getReasonCodes() {
    return delegate.getReasonCodes();
  }

}
