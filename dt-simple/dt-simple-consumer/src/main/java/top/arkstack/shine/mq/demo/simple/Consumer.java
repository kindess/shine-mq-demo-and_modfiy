package top.arkstack.shine.mq.demo.simple;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import top.arkstack.shine.mq.RabbitmqFactory;
import top.arkstack.shine.mq.bean.SendTypeEnum;
import top.arkstack.shine.mq.constant.MqConstant;
import top.arkstack.shine.mq.demo.simple.processor.ProcessorException;
import top.arkstack.shine.mq.demo.simple.processor.ProcessorTest;

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
    RabbitmqFactory factory;

    @Autowired
    private ProcessorException exception;

    /**
     * 消费者回调，监听到消息后业务处理逻辑
     */
    @Autowired
    private ProcessorTest processorTest;

    /**
     * 构造器 => PostConstruct => init初始化
     */
    @PostConstruct
    public void test() {
        //服务B 配置消费者
        factory.addDLX("simple_route_config", "simple_route_config",
                "simple_route_config_key", processorTest, SendTypeEnum.DISTRIBUTED);

        //配置死信队列 失败时候处理
        factory.add(MqConstant.DEAD_LETTER_QUEUE, MqConstant.DEAD_LETTER_EXCHANGE,
                MqConstant.DEAD_LETTER_ROUTEKEY, exception, SendTypeEnum.DLX);
    }
}
