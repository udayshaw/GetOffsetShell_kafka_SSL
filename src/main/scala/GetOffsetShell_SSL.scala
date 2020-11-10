package uday.kafkaTools

import joptsimple._
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties
import java.time.Instant
import collection.JavaConverters._
import collection.JavaConversions._
import scala.util.parsing.json._
import scala.util.Try
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.clients.admin.{AdminClient,NewTopic,AdminClientConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

object GetOffsetShell_SSL {
  def main(args: Array[String]): Unit = {

    val parser = new OptionParser
    val kafkaBrokersOpt = parser.accepts("broker-list", "REQUIRED: The hostname of the server to connect to.").withRequiredArg.describedAs("kafka://hostname:port").ofType(classOf[String])
    val inputTopicOpt = parser.accepts("topic", "REQUIRED: The topic to get offset from.").withRequiredArg.describedAs("topic").ofType(classOf[String])
    val securityProtocolOpt = parser.accepts("security.protocol", "REQUIRED: The security protocol to be used to Login.").withRequiredArg.describedAs("security.protocol").ofType(classOf[String])
    val sslTruststoreLocationOpt = parser.accepts("ssl.truststore.location", "REQUIRED: ssl truststore location.").withRequiredArg.describedAs("ssl.truststore.location").ofType(classOf[String])
    val sslTruststorePasswordOpt = parser.accepts("ssl.truststore.password", "REQUIRED: encrypted ssl truststore password.").withRequiredArg.describedAs("ssl.truststore.password").ofType(classOf[String])
    val group_id="testgroup"

    val options = parser.parse(args : _*)
    val kafkaBrokers = options.valueOf(kafkaBrokersOpt)
    val inputTopic = options.valueOf(inputTopicOpt)
    val security_protocol = options.valueOf(securityProtocolOpt)
    val ssl_truststore_location = options.valueOf(sslTruststoreLocationOpt)
    val ssl_truststore_password = options.valueOf(sslTruststorePasswordOpt)
    val kafkaParams : java.util.Map[String,Object] = scala.collection.immutable.Map[String, Object](
                               "bootstrap.servers" -> kafkaBrokers,
                               "key.deserializer" -> classOf[StringDeserializer],
                               "value.deserializer" -> classOf[StringDeserializer],
                               "group.id" -> group_id,
                               "auto.offset.reset" -> "earliest",
                               "security.protocol" -> security_protocol,
                               "ssl.truststore.location" -> ssl_truststore_location,
                               "ssl.truststore.password" -> ssl_truststore_password,
                               "enable.auto.commit" -> (false: java.lang.Boolean)
                               ).asJava
    val KafkaConfig = new Properties()

    KafkaConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    KafkaConfig.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, security_protocol)
    KafkaConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ssl_truststore_location)
    KafkaConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ssl_truststore_password)
    val adminClient = AdminClient.create(KafkaConfig)

    val consumer = new KafkaConsumer[String,String](kafkaParams)
    val topicPartitions = adminClient.describeTopics(List[String](inputTopic).asJava).all().get().get(inputTopic).partitions()
    val offsetRanges = topicPartitions.asScala.map(x =>{
          val topicPartition = new TopicPartition(inputTopic, x.partition)
          val stopOffset = consumer.endOffsets(List[TopicPartition](topicPartition)).values().asScala.toList.get(0)
          (topicPartition+":"+stopOffset).reverse.replaceFirst("-",":").reverse
        }).toArray
    for(i <- offsetRanges){
      println(i)
    }

  }
}
