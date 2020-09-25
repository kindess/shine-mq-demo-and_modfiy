package top.arkstack.shine.mq.demo.rollback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import top.arkstack.shine.mq.bean.TransferBean;
import top.arkstack.shine.mq.demo.dao.RouteConfigMapper;
import top.arkstack.shine.mq.processor.BaseProcessor;


/**
 * route_config的异常回滚
 * 回滚执行失败怎么办？业务需要执行回滚，比如库存为0了，需要取下订单和退款，但是每次重试回滚消息都丢失，回滚不会被调用怎么办？
 * Daemon会定时查询redis
 *
 * @author 7le
 * @version 2.2.0
 */
@Slf4j
@Component
public class RouteConfigRollback extends BaseProcessor {

    @Autowired
    private RouteConfigMapper mapper;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Object process(Object msg, Message message, Channel channel) throws Exception {
        log.info("route_config rollback :{}", msg);
//        TransferBean bean = JSONObject.parseObject(msg.toString(), TransferBean.class);
        TransferBean bean = new ObjectMapper().readValue(msg.toString(), TransferBean.class);
        //rollback transaction
        // 1、生产者监听回滚队列，进行业务数据回滚
        mapper.deleteByPrimaryKey(Long.valueOf(bean.getCheckBackId()));
        /**
         *
         * channel.BasicQos(0, 1, false);
         * 第二个参数是处理消息最大的数量。举个例子，如果输入1，那如果接收一个消息，但是没有应答，则客户端不会收到下一个消息，消息只会在队列中阻塞。
         *
         * 另外，消息的确认类型：
         *
         * channel.basicAck(deliveryTag, multiple);
         * consumer处理成功后，通知broker删除队列中的消息，如果设置multiple=true，表示支持批量确认机制以减少网络流量。
         * 例如：有值为5,6,7,8 deliveryTag的投递
         * 如果此时channel.basicAck(8, true);则表示前面未确认的5,6,7投递也一起确认处理完毕。
         * 如果此时channel.basicAck(8, false);则仅表示deliveryTag=8的消息已经成功处理。
         *
         * channel.basicNack(deliveryTag, multiple, requeue);
         * consumer处理失败后，例如：有值为5,6,7,8 deliveryTag的投递。
         * 如果channel.basicNack(8, true, true);表示deliveryTag=8之前未确认的消息都处理失败且将这些消息重新放回队列中。
         * 如果channel.basicNack(8, true, false);表示deliveryTag=8之前未确认的消息都处理失败且将这些消息直接丢弃。
         * 如果channel.basicNack(8, false, true);表示deliveryTag=8的消息处理失败且将该消息重新放回队列。
         * 如果channel.basicNack(8, false, false);表示deliveryTag=8的消息处理失败且将该消息直接丢弃。
         *
         * channel.basicReject(deliveryTag, requeue);
         * 相比channel.basicNack，除了没有multiple批量确认机制之外，其他语义完全一样。
         * 如果channel.basicReject(8, true);表示deliveryTag=8的消息处理失败且将该消息重新放回队列。
         * 如果channel.basicReject(8, false);表示deliveryTag=8的消息处理失败且将该消息直接丢弃。
         *
         * 由此可见，手动Ack如果处理方式不对会发生一些问题。
         * 1.没有及时ack，或者程序出现bug，所有的消息将被存在unacked中，消耗内存
         * 如果忘记了ack，那么后果很严重。当Consumer退出时，Message会重新分发。然后RabbitMQ会占用越来越多的内存，由于 RabbitMQ会长时间运行，因此这个“内存泄漏”是致命的。
         * 2.如果使用BasicNack，将消费失败的消息重新塞进队列的头部，则会造成死循环。
         * （解决basicNack造成的消息循环循环消费的办法是为队列设置“回退队列”，设置回退队列和阀值，如设置队列为q1，阀值为2，则在rollback两次后将消息转入q1）
         *
         * 综上，手动ack需要注意的是：
         * 1.在消费者端一定要进行ack，或者是nack，可以放在try方法块的finally中执行
         * 2.可以对消费者的异常状态进行捕捉，根据异常类型选择ack，或者nack抛弃消息，nack再次尝试
         * 3.对于nack的再次尝试，是进入到队列头的，如果一直是失败的状态，将会造成阻塞。所以最好是专门投递到“死信队列”，具体分析。
         */
        // 2、手动回执ack，消费者完成消费，通知MQ删除消息
        channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
        return null;
    }
}
