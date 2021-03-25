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
package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {

  private final KafkaProducer<Integer, String> producer;
  private final String topic;
  private final Boolean isAsync;

  public Producer(String topic, Boolean isAsync) {
    Properties props = new Properties();
    props.put("bootstrap.servers",
        KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
    props.put("client.id", "DemoProducer");
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producer = new KafkaProducer<>(props);
    this.topic = topic;
    this.isAsync = isAsync;
  }

  @Override
  public void run() {
    //偏移量
    int messageNo = 1;
    while (true) {
      String messageStr = "Message_" + messageNo;
      long startTime = System.currentTimeMillis();
      ProducerRecord producerRecord = new ProducerRecord<>(topic, messageNo, messageStr);
      if (isAsync) {
        // Send asynchronously。异步发送
        // 提供一个回调，调用 send后可以继续发送消息而不用等待 。 当有结果运回时，会向动执行回调函数
        DemoCallBack callBack = new DemoCallBack(startTime, messageNo, messageStr);
        producer.send(producerRecord, callBack);
      } else {
        // Send synchronously。同步发送
        try {
          //  需要立即调用get，因为Future.get在没有返回结果时会一直阻塞
          producer.send(producerRecord).get();
          System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
      // 偏移量递增
      ++messageNo;
    }
  }
}

class DemoCallBack implements Callback {

  private final long startTime;
  private final int key;
  private final String message;

  public DemoCallBack(long startTime, int key, String message) {
    this.startTime = startTime;
    this.key = key;
    this.message = message;
  }

  /**
   * A callback method the user can implement to provide asynchronous handling of request
   * completion. This method will
   * be called when the record sent to the server has been acknowledged. Exactly one of the
   * arguments will be
   * non-null.
   * <p>
   * 完成后所做的后续操作
   *
   * @param metadata  The metadata for the record that was sent (i.e. the partition and offset).
   *                  Null if an error
   *                  occurred.
   * @param exception The exception thrown during processing of this record. Null if no error
   *                  occurred.
   */
  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    long elapsedTime = System.currentTimeMillis() - startTime;
    if (metadata != null) {
      System.out.println(
          "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
              "), " +
              "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
    } else {
      exception.printStackTrace();
    }
  }
}
