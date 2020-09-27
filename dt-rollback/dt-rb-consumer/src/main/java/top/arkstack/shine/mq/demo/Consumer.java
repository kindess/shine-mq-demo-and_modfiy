package top.arkstack.shine.mq.demo;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import top.arkstack.shine.mq.RabbitmqFactory;
import top.arkstack.shine.mq.bean.SendTypeEnum;
import top.arkstack.shine.mq.constant.MqConstant;
import top.arkstack.shine.mq.demo.processor.ProcessorException;
import top.arkstack.shine.mq.demo.processor.ProcessorTest;
import top.arkstack.shine.mq.processor.BaseProcessor;

import javax.annotation.PostConstruct;

/**
 * 服务B 消费者处理消息
 *
 * @author 7le
 * @version 1.0.0
 */
@Slf4j
@Component
public class Consumer {

    @Autowired
    private RabbitmqFactory factory;

    /**
     * 处理死信
     */
    @Autowired
    private ProcessorException exception;

    @Autowired
    private ProcessorTest processorTest;


    @PostConstruct
    public void test() {
        //服务B 配置消费者
        // 1、申明交换机、队列绑定关系   2、监听此队列，当监听到消息，使用processorTest进行处理
        factory.addDLX("route_config", "route_config",
                "route_config_key", processorTest, SendTypeEnum.DISTRIBUTED,
                "route_config" + MqConstant.SPLIT + MqConstant.DEAD_LETTER_EXCHANGE,
                MqConstant.DEAD_LETTER_ROUTEKEY);

        //给指定死信交换机配置死信队列，并监听死信队列，失败时候处理
        factory.add("route_config" + MqConstant.SPLIT + MqConstant.DEAD_LETTER_QUEUE,
                "route_config" + MqConstant.SPLIT + MqConstant.DEAD_LETTER_EXCHANGE,
                MqConstant.DEAD_LETTER_ROUTEKEY, exception, SendTypeEnum.DLX);
    }
}
