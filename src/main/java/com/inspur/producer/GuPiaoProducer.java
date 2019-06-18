package com.inspur.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.Random;


/**
 * 单线程的股票生产者
 * @author YangJianWei
 */
public class GuPiaoProducer {
	
	private static Logger LOG = Logger.getLogger(GuPiaoProducer.class);
	private static String BROKER_LIST = "192.168.120.110:9092,192.168.120.150:9092,192.168.120.224:9092";
//	private static String TOPIC = "GuPiaoHangQing";
	private static String TOPIC = "topic-blance";
	private static int MSG_SIZE = 100;
	
	private static Properties producerConfig = new Properties();
	private static KafkaProducer<String, String> kafkaProducer = null;
	
	
	static {
		//服务器节点的匹配
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
		//key序列化规则器
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//value序列化规则器
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//在生产者中使用自定义的分区器!
		//producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.inspur.partition.MyPartitioner");

		kafkaProducer = new KafkaProducer<String, String>(producerConfig);
	}
	
	/**
	 * 创建股票信息
	 * @return
	 */
	private static GuPiao createGuPiaoInfo(){
		GuPiao gupiao = new GuPiao();
		Random random = new Random();
		int price = random.nextInt(100) + 1;
		
		gupiao.setCurrentTime(System.currentTimeMillis());
		gupiao.setGuPiaoName("中国深蓝科技");
		gupiao.setGuPiaoNickName("CDBT1");
		gupiao.setGuPiaoPrice(price);
		gupiao.setGuPiaoDesc("深蓝科技股票");
		
		return gupiao;
	}

	public static void sendMessage() {
		ProducerRecord<String, String> record = null;
		GuPiao gupiao = null;
		int num = 1;
		try {
			for(int i = 0; i < MSG_SIZE; i++){
				gupiao = createGuPiaoInfo();
				/**
				 * 设置不同的key 测试 分区管理器是否进行不同的路由存放
				 */
//				record = new ProducerRecord<String, String>(TOPIC, null, gupiao.getCurrentTime(),
//						 gupiao.getGuPiaoNickName(), gupiao.toString());
				record = new ProducerRecord<String, String>(TOPIC, null, gupiao.getCurrentTime(),
						 null, gupiao.toString());
				kafkaProducer.send(record);//发送并遗忘!
				if(num++ % 100 == 0	){
					Thread.currentThread().sleep(5000L);		//每100条消息记录线程休息2s
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
		} finally {
			kafkaProducer.close();
		}
	}
	
	/**
	 * 有回调的生产者进行产生消息,发送消息是可以记录日志,或打印日志之类的操作!
	 */
	public static void sendMessageWithCallBack(){
		ProducerRecord<String, String> record = null;
		GuPiao gupiao = null;
		int num = 1;
		try {
			for(int i = 0; i < MSG_SIZE; i++){
				gupiao = createGuPiaoInfo();
				record = new ProducerRecord<String, String>(TOPIC, null, gupiao.getCurrentTime(), 
						 gupiao.getGuPiaoNickName(), gupiao.toString());
				kafkaProducer.send(record, new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null){
							LOG.error(exception.getMessage());
						}
						if(metadata != null){
							System.out.println("topic ---:" + metadata.topic() + ",partition ---:" + metadata.partition() + ",offset ---:" + metadata.offset());
						}
					}
				});
				if(num++ % 100 == 0){
					Thread.currentThread().sleep(2000L);
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
		} finally {
			kafkaProducer.close();
		}
	}
	

	public static void main(String[] args) {
		sendMessage();
//		sendMessageWithCallBack();
	}
}
