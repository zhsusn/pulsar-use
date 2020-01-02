package org.psn.tests;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.concurrent.TimeUnit;

public class TestConsume {
    static String url = "pulsar://192.168.60.96:6650,192.168.60.97:6650,192.168.60.98:6650,192.168.60.99:6650";
    static String topicName = "persistent://tenant01/np01/xp01";

    public static void main(String[] args) throws Exception {
      // produceMsg();
//        try{
//            Thread.sleep(10000);
//        }catch(Exception e){
//            System.exit(0);//退出程序
//        }
        System.out.println("begin to consume msg linsten !");

        consumeMsgWithAutoConsume();

    }

    /**
     * pass
     */
    private static void consumeMsgSimple() {
        PulsarClient client = null;
        try {
           client = PulsarClient.builder().serviceUrl(url).build();
            Consumer consumer = client.newConsumer().topic(topicName).subscriptionName("consumer01").subscribe();
            while(true){
                Message msg = consumer.receive();
                try {
                    // Do something with the message
                    System.out.printf("Message received: %s", new String(msg.getData()));
                    System.out.println("");
                    // Acknowledge the message so that it can be deleted by the message broker
                    consumer.acknowledge(msg);
                } catch (Exception e) {
                    // Message failed to process, redeliver later
                    consumer.negativeAcknowledge(msg);
                }
            }
        } catch (Throwable t) {
            System.out.println("Got error" + t.getMessage());
        } finally {
            if (null != client) {
                try {
                    client.close();
                } catch (PulsarClientException e) {
                    System.out.println("Failed to close test client" + e.getMessage());
                }
            }
        }
    }

    /**
     * pass
     */
    private static void consumeMsgWithSchema() {
        PulsarClient client = null;
        try {
            client = PulsarClient.builder().serviceUrl(url).build();
            GenericSchema<GenericRecord>  genericSchema=getSchema();

            Consumer  consumer = client.newConsumer(genericSchema) //
                    .receiverQueueSize(2000) //
                    .acknowledgmentGroupTime(200, TimeUnit.MILLISECONDS) //
                    .subscriptionType(SubscriptionType.Failover)
                    .topic(topicName).subscriptionName("consumer01").subscribe();
            while(true){
                Message msg = consumer.receive();
                try {
                    // Do something with the message
                    System.out.printf("Message received: %s", new String(msg.getData()));
                    System.out.println("");
                    // Acknowledge the message so that it can be deleted by the message broker
                    consumer.acknowledge(msg);
                } catch (Exception e) {
                    // Message failed to process, redeliver later
                    consumer.negativeAcknowledge(msg);
                }
            }
        } catch (Throwable t) {
            System.out.println("Got error" + t.getMessage());
        } finally {
            if (null != client) {
                try {
                    client.close();
                } catch (PulsarClientException e) {
                    System.out.println("Failed to close test client" + e.getMessage());
                }
            }
        }
    }

    /**
     * pass
     */
    private  static void consumeMsgWithAutoConsume(){
        MessageListener<GenericRecord> listener = (consumer, msg) -> {
            System.out.println("====== begin to AutoConsume data" );
            System.out.printf("Message received: %s", msg.getValue());
            GenericRecord rec= msg.getValue();
            System.out.println("Field Lists are :"+rec.getFields().toString());
            System.out.println(rec.getFields().get(0)+":"+rec.getField(rec.getFields().get(0)));
            consumer.acknowledgeAsync(msg);
        };

        ClientBuilder clientBuilder = PulsarClient.builder() //
                .serviceUrl(url) //
                .ioThreads(Runtime.getRuntime().availableProcessors());

        PulsarClient pulsarClient = null;
        try {
            pulsarClient = clientBuilder.build();

        ConsumerBuilder<GenericRecord> consumerBuilder = pulsarClient.newConsumer(Schema.AUTO_CONSUME()) //
                .messageListener(listener) //
                .acknowledgmentGroupTime(200, TimeUnit.MILLISECONDS) //
                .subscriptionType(SubscriptionType.Exclusive)
                .replicateSubscriptionState(false);
            System.out.println("====== consturct consumer=====");
            consumerBuilder.topic(topicName.toString()).subscriptionName("consumer01")
                        .subscribeAsync();
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    break;
                }
            }

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }  finally {
        if (null != pulsarClient) {
            try {
                pulsarClient.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
                System.out.println("Failed to close test client" + e.getMessage());
            }
        }
    }
    }

    /**
     * Pass
     */
    private static void consumeMsgWithListener() {
        PulsarClient client = null;
        try {
            MessageListener<GenericRecord> listener = (consumer, msg) -> {

                System.out.printf("Message received: %s", new String(msg.getData()));
                System.out.println(" \n   consume data");
                    try {
                        // Do something with the message
                        // Acknowledge the message so that it can be deleted by the message broker
                        consumer.acknowledge(msg);
                        //  consumer.acknowledgeAsync(msg);
                    } catch (Exception e) {
                        // Message failed to process, redeliver later
                        consumer.negativeAcknowledge(msg);
                    }


            };
            ClientBuilder clientBuilder = PulsarClient.builder() //
                    .serviceUrl(url); //
                  //  .connectionsPerBroker(200) //
                  //  .ioThreads(Runtime.getRuntime().availableProcessors());
            client = clientBuilder.build();
            GenericSchema<GenericRecord>  genericSchema=getSchema();

            ConsumerBuilder<GenericRecord> consumerBuilder = client.newConsumer(genericSchema) //
                    .messageListener(listener) //
                    .receiverQueueSize(2000) //
                    .acknowledgmentGroupTime(200, TimeUnit.MILLISECONDS) //
                    .subscriptionType(SubscriptionType.Failover)
                    .replicateSubscriptionState(true);
            consumerBuilder.topic(topicName).subscriptionName("consumer01").subscribeAsync();

            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    break;
                }
            }


        } catch (Throwable t) {
            t.printStackTrace();
            System.out.println("Got error" + t.getMessage());
        } finally {
            if (null != client) {
                try {
                    client.close();
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                    System.out.println("Failed to close test client" + e.getMessage());
                }
            }
        }
    }

    private static GenericSchema<GenericRecord>  getSchema(){
        //Schema结构定义
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
        recordSchemaBuilder.field("sname").type(SchemaType.STRING);
        // recordSchemaBuilder.field("age").type(SchemaType.INT16);
        recordSchemaBuilder.field("salary").type(SchemaType.DOUBLE);
        // recordSchemaBuilder.field("snote").type(SchemaType.JSON);
        //recordSchemaBuilder.field("dbirthday").type(SchemaType.DATE);
        //定义Schema封装格式
        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord>  records=  Schema.generic(schemaInfo);
        return records;
    }

    /**
     * pass
     */
    private static void produceMsg() {
        PulsarClient client = null;
        try {

            ClientBuilder clientBuilder = PulsarClient.builder() //
                    .serviceUrl(url) //
                    .connectionsPerBroker(20) //
                    .ioThreads(Runtime.getRuntime().availableProcessors());

            client = clientBuilder.build();
            //构造行
            GenericSchema<GenericRecord>  recoder = getSchema();

            Producer producer = client.newProducer(recoder).topic(topicName).create();
            System.out.println("######开始发送数据######");
            for (int i = 0; i < 100; i++) {
                //赋值.set("age",18) .set("snote","{\"name\": \"“给我唱首歌”\\n“给我讲个故事”\" }")
                GenericRecord rec = recoder.newRecordBuilder().set("sname", "张三").set("salary", 32895.8).build();
              System.out.println("Produce No "+ i+ " data");
                producer.newMessage().value(rec).sendAsync();
            }


//            Consumer consumer = client.newConsumer(Schema.AUTO_CONSUME()).subscribe();
//            Message msg  = consumer.receive();
//            GenericRecord record = msg.getValue();


        } catch (Throwable t) {
            System.out.println("Got error" + t.getMessage());
        } finally {
            if (null != client) {
                try {
                    client.close();
                } catch (PulsarClientException e) {
                    System.out.println("Failed to close test client" + e.getMessage());
                }
            }
        }
    }
}
