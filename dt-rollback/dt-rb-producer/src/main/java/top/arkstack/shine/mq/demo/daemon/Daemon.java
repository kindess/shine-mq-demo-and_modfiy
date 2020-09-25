package top.arkstack.shine.mq.demo.daemon;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import top.arkstack.shine.mq.bean.EventMessage;
import top.arkstack.shine.mq.bean.PrepareMessage;
import top.arkstack.shine.mq.bean.TransferBean;
import top.arkstack.shine.mq.coordinator.Coordinator;
import top.arkstack.shine.mq.coordinator.redis.RedisUtil;
import top.arkstack.shine.mq.demo.dao.RouteConfigMapper;
import top.arkstack.shine.mq.demo.dao.model.RouteConfig;

import java.util.List;
import java.util.Objects;

/**
 * 守护线程
 * 这里的demo简单的将守护线程做到服务A中（使用定时任务）。
 * 也可以单独做一个服务或者其他方式。
 *
 * @author 7le
 * @version 1.0.0
 */
@Slf4j
@Component
public class Daemon {

    @Autowired
    private Coordinator coordinator;

    @Autowired
    private RouteConfigMapper mapper;

    @Autowired
    private RedisUtil redisUtil;

    @Scheduled(initialDelay = 5_000, fixedRate = 30_000)
    public void process() {
        try {
            //处理Prepare消息 如果在集群情况下，是有可能出现重复消息的，这里演示使用分布式锁
            //  TODO 相同的微服务不必要重复补偿，否则会产生大量消息导致消息堆积，同时还是重复的消息。不同生产者服务应该处理各自服务的消息，因此需要用不同的锁锁住各自的补偿定时任务
            redisUtil.lock("redis_lock", 90_000L, () -> {

                /**
                 * 服务A 的任务
                 * 1、缓存预消息
                 * 2、业务（加了事务注解，第一步出错会抛出异常，不会继续执行；而第二步异常，抛出异常，业务回滚；第三步出错，抛出异常，业务回滚，发送消息到MQ失败）
                 * 3、注解DistributedTrans的切面将预消息发送到MQ
                 *
                 * 可能的情况：
                 *      a、第一步就出错。此处直接查不到缓存（不考虑）
                 *      b、第二步出错，有缓存，业务回滚
                 *      c、第三步出错，有缓存，业务回滚，投递消息时抛出运行异常，生产者根本没投递出去
                 *      d、执行成功，有缓存，有业务数据，已投递（MQ收到，但是ack丢失，导致确认回调函数不执行，通信阶段出现问题）
                 *      e、完全执行成功，无任何异常
                 */
                List<PrepareMessage> prepare = coordinator.getPrepare();
                if (!Objects.isNull(prepare) && prepare.size() > 0) {
                    prepare.forEach(p -> {
                        //可以首先根据p.getBizId()获取业务id进行分类
                        //再根据回查id（这里是用数据库记录唯一键，主键、不重复订单号等等，可以根据自己的业务场景采用其他方式）查询服务A的该任务是否已经完成，
                        RouteConfig config = mapper.selectByPrimaryKey(Long.valueOf(p.getCheckBackId()));
                        // 异常b、有缓存，mysql业务回滚(数据一致)
                        if (Objects.isNull(config)) {
                            log.info("服务A中任务并没有完成，CheckBackId:{}", p);
                            //因为服务A的任务没有完成，所以这次操作就是失败了，可以记录下日志，这时候数据是一致的
                            coordinator.delPrepare(p.getCheckBackId());
                        } else {
                            // 异常d、有缓存，有业务数据，MQ无消息
                            log.info("服务A中任务已经完成，CheckBackId:{}", p);
                            //服务A的任务已经完成，但是prepare消息没被删除，说明投递到mq失败了，那就继续进行投递或者将任务回滚
                            // TODO 这里MQ投递失败分两种情况，1是投递到MQ之前丢失（或者MQ丢失消息同理） 2是MQ回调生产者的ack丢失（或者回调执行时抛出异常）
                            try {
                                //如果要任务回滚，可以按照业务自行回滚
                                //如果进行投递,有需要传递信息，则需要重新加上，这里演示继续投递，模拟之前的data
                                // 因为预消息可能是没有消息体，此处需要自己重新封装消息体
                                p.setData(new TransferBean(p.getCheckBackId(), config.getPath()));
                                coordinator.compensatePrepare(p);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
                return null;
            });
            //处理ready消息
            redisUtil.lock("redis_lock_ready", 90_000L, () -> {
                List<EventMessage> ready = coordinator.getReady();
                if (!Objects.isNull(ready) && ready.size() > 0) {
                    ready.forEach(r -> {
                        //超时的ready的消息，就直接捞起发送到消息中间件，因为只要是ready消息持久化到协调者，那就说明服务A的任务已经完成。
                        //因为消息到mq是异步通知的，所以补偿的频率过高会造成消息重复，下游服务最好能保证幂等性
                        try {
                            // 重发补偿消息
                            coordinator.compensateReady(r);
                            log.info("重新投递消息： {}", r);
                        } catch (Exception e) {
                            log.error("Message failed to be sent : ", e);
                        }
                    });
                }
                return null;
            });

        } catch (Exception e) {
            log.error("daemon process error: ", e);
        }
    }
}
