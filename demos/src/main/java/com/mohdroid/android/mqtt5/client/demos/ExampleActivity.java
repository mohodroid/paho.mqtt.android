package com.mohdroid.android.mqtt5.client.demos;

import android.app.Activity;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import com.mohdroid.android.mqtt5.client.core.MqttAndroidClient;
import com.mohdroid.android.mqtt5.client.core.MqttService;
import com.mohdroid.android.mqtt5.client.core.MqttTraceHandler;

import org.eclipse.paho.mqttv5.client.DisconnectedBufferOptions;
import org.eclipse.paho.mqttv5.client.IMqttDeliveryToken;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttActionListener;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

public class ExampleActivity extends Activity {

    private static final String TAG = "ExampleActivity";
    private String clientId = "exampleClientId";
    private final String subscriptionTopic = "exampleTopic";
    private final String publishTopic = "exampleTopic";
    private String publishMessage = "publishMessage";
    private final String serverUri = "tcp://192.168.0.249:1883";
    private MqttAndroidClient mqttAndroidClient;

    //widget
    private Button btnConnect, btnPublish;
    private TextView tvMessage;
    private EditText etMessage;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_example);
        btnConnect = findViewById(R.id.btn_connect);
        btnPublish = findViewById(R.id.btn_publish);
        tvMessage = findViewById(R.id.tv_message);
        etMessage = findViewById(R.id.et_message);
        enableBtnPublish(false);
        clientId = clientId + System.currentTimeMillis();
        mqttAndroidClient = new MqttAndroidClient(this, serverUri, clientId);
        mqttAndroidClient.setCallback(mqttCallback);
        mqttAndroidClient.setTraceEnabled(true);
        mqttAndroidClient.setTraceCallback(new MqttTraceCallback());
        final MqttConnectionOptions mqttConnectionOptions = new MqttConnectionOptions();
        mqttConnectionOptions.setCleanStart(true);
        mqttConnectionOptions.setAutomaticReconnect(true);

        btnConnect.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                try {
                    IMqttToken connectionToken = mqttAndroidClient.connect(mqttConnectionOptions, null, new MqttConnectionListener());
                } catch (MqttException e) {
                    Log.d(TAG, "Error connecting: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        });

        btnPublish.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                try {
                    IMqttDeliveryToken publishToken = publishMessage();
                } catch (MqttException e) {
                    Log.d(TAG, "Error Publishing: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        });
    }

    private IMqttDeliveryToken publishMessage() throws MqttException {
        publishMessage = etMessage.getText().toString().trim();
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(publishMessage.getBytes());
        mqttMessage.setQos(2);
        IMqttDeliveryToken publishToken = mqttAndroidClient.publish(publishTopic, mqttMessage, null, new MqttPublishListener());
        if (!mqttAndroidClient.isConnected()) {
            Log.d(TAG, mqttAndroidClient.getBufferedMessageCount() + " messages in buffer.");
        }
        return publishToken;
    }

    private IMqttToken subscribeToTopic() {
        IMqttToken subscribe = null;
        try {
            subscribe = mqttAndroidClient.subscribe(subscriptionTopic, 2, null, new MqttSubscribeListener());
        } catch (MqttException e) {
            Log.d(TAG, "Exception whilst subscribing" + e.getMessage());
            e.printStackTrace();
        }
        return subscribe;
    }

    private void enableBtnPublish(boolean enable) {
        btnPublish.setEnabled(enable);
        btnPublish.setClickable(enable);
    }

    private MqttCallback mqttCallback = new MqttCallback() {
        @Override
        public void disconnected(MqttDisconnectResponse disconnectResponse) {
            Throwable why = disconnectResponse.getException().getCause();
            Log.d(TAG, "The connection was lost!" + why);
        }

        @Override
        public void mqttErrorOccurred(MqttException exception) {

        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            String messageArrived = new String(message.getPayload());
            Log.d(TAG, "Incoming message: " + messageArrived);
            tvMessage.setText(messageArrived);
        }

        @Override
        public void deliveryComplete(IMqttToken token) {
            try {
                Log.d(TAG, "delivered message: " + token.getMessage());
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            if (reconnect) {
                Log.d(TAG, "reconnected to " + serverURI);
                subscribeToTopic();
            } else {
                Log.d(TAG, "Connected to " + serverURI);
            }
        }

        @Override
        public void authPacketArrived(int reasonCode, MqttProperties properties) {

        }
    };

    static class MqttTraceCallback implements MqttTraceHandler {

        public void traceDebug(java.lang.String arg0, java.lang.String arg1) {
            Log.i(arg0, arg1);
        }

        public void traceError(java.lang.String arg0, java.lang.String arg1) {
            Log.e(arg0, arg1);
        }

        public void traceException(java.lang.String arg0, java.lang.String arg1,
                                   java.lang.Exception arg2) {
            Log.e(arg0, arg1, arg2);
        }

    }

    class MqttConnectionListener implements MqttActionListener {

        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
            Log.d(TAG, "success connect to server: " + serverUri);
            DisconnectedBufferOptions disconnectedBufferOptions = new DisconnectedBufferOptions();
            disconnectedBufferOptions.setBufferEnabled(true);
            disconnectedBufferOptions.setBufferSize(100);
            disconnectedBufferOptions.setPersistBuffer(false);
            disconnectedBufferOptions.setDeleteOldestMessages(false);
            mqttAndroidClient.setBufferOpts(disconnectedBufferOptions);
            enableBtnPublish(true);
            IMqttToken iMqttToken = subscribeToTopic();
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
            Log.d(TAG, "failed connect to server: " + serverUri + "cause by " + exception.getMessage());
            enableBtnPublish(false);
        }
    }

    static class MqttPublishListener implements MqttActionListener {

        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
            Log.d(TAG, "published!");
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
            Log.d(TAG, "failed to published!");
        }
    }

    static class MqttSubscribeListener implements MqttActionListener {
        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
            Log.d(TAG, "subscribed!");
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
            Log.d(TAG, "failed to subscribed!");
        }
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        try {
            mqttAndroidClient.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}