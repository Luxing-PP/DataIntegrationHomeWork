import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.*;
import java.sql.Time;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class main {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.29.4.17:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // GROUP_ID请使用学号，不同组应该使用不同的GROUP。
        // 原因参考：https://blog.csdn.net/daiyutage/article/details/70599433
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "191250009");

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"student\" password=\"nju2022\";");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList("transaction"));
        File recordFile = new File("record_new.txt");
        FileWriter fileWriter = new FileWriter(recordFile,true);
        // 会从最新数据开始消费
        long time = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                time++;
                fileWriter.write("value:"+record.value()+"\n");
                if(time%500==0){
                    System.out.println("time:"+time+new Date().toString());
                }
            }
            fileWriter.flush();
        }
    }
}
