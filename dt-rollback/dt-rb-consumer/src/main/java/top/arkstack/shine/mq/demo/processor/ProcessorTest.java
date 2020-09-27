package top.arkstack.shine.mq.demo.processor;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import top.arkstack.shine.mq.bean.TransferBean;
import top.arkstack.shine.mq.demo.dao.RouteConfigMapper;
import top.arkstack.shine.mq.demo.dao.model.RouteConfig;
import top.arkstack.shine.mq.processor.BaseProcessor;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/**
 * 服务B 执行分布式事务（route）
 *
 * @author 7le
 */
@Slf4j
@Component
public class ProcessorTest extends BaseProcessor {

    @Autowired
    private RouteConfigMapper mapper;
    /**
     * 加上事务注解，当消费者处理不成功时，业务数据回退
     * @param msg
     * @param message
     * @param channel
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public Object process(Object msg, Message message, Channel channel) throws IOException {
        //执行服务B的任务  这里可以将msg转成TransferBean
        if (!Objects.isNull(msg)) {
                TransferBean bean = JSONObject.parseObject(msg.toString(), TransferBean.class);
            // TODO  msg是缺少双引号的字符串，使用jackson转换成Json对象会抛异常，只能使用fastJson
//            TransferBean bean = new ObjectMapper().readValue(msg.toString(), TransferBean.class);
            //这里就可以处理服务B的任务了
            log.info("(Route_config) Process task B : {}", bean.getData());
            // checkBackId是保证消费者幂等性的关键，假设是订单主键，而当前服务是库存服务
            // 由于库存的增减是所有库存服务消费者（多库存服务）会访问到的，是不能同时处理的
            /**
             * TODO 1、通过SET key value [NX|XX]命令，消费者已处理的flag标志存在不操作，不存在则进行库存减少（PS：redis集群下，避免不了主从库数据不一致，导致查询数据有问题，因此，此库存服务必须读写主库）
             * TODO 此处对为什么要使用setnx命令判断说明一下，因为redis是支持多连接，在多连接情况依然出现命令交叉执行，导致数据不一致，使用setnx保证多连接也能原子执行。只有此办法才能保证库存服务幂等性
             */
            //            if (!flag){
            // 2、消费处理
            log.info("(Route_config) CheckBackId : {}", bean.getCheckBackId());
            //            }
        }
        //执行操作
        // 此处Id使用当前的时间戳，仅可用来测试是否回滚
        RouteConfig routeConfig = new RouteConfig(System.currentTimeMillis(), "/shine/**", "spring-mq",
                null, false, true, true, null);
        mapper.insert(routeConfig);
        //模拟失败进入回滚
                int i = 1 / 0;
        //分布式事务消息默认自动回执

        // 消费之后，记得要应答，队列才会移除消息，不然一直是nack状态.
        // 本例中，生产者使用@DistributedTrans发送事务消息，因此此处可以不用手动应答。由MessageAdapterHandler处理，决定是否重试还是删除此消息，重新发送一条回滚消息。即使手动应答也不会受到影响
//        channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
        return null;
    }
}
