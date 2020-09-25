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

    // 协调者，默认实现了RedisCoordinator，可自定义实现Coordinator接口
    @Autowired
    private Coordinator coordinator;

    @Autowired
    private RouteConfigMapper mapper;

    @Autowired
    private RedisUtil redisUtil;

    /**
     * 1、fixedDelay控制方法执行的间隔时间，是以上一次方法执行完开始算起，如上一次方法执行阻塞住了，那么直到上一次执行完，并间隔给定的时间后，执行下一次。
     *
     * 2、fixedRate是按照一定的速率执行，是从上一次方法执行开始的时间算起，如果上一次方法阻塞住了，下一次也是不会执行，但是在阻塞这段时间内累计应该执行的次数，当不再阻塞时，一下子把这些全部执行掉，而后再按照固定速率继续执行。
     *
     * 3、cron表达式可以定制化执行任务，但是执行的方式是与fixedDelay相近的，也是会按照上一次方法结束时间开始算起。
     *
     * 4、initialDelay 。如： @Scheduled(initialDelay = 10000,fixedRate = 15000
     * 这个定时器就是在上一个的基础上加了一个initialDelay = 10000 意思就是在容器启动后,延迟10秒后再执行一次定时器,以后每15秒再执行一次该定时器。
     */
    @Scheduled(initialDelay = 5_000, fixedRate = 30_000)
    public void process() {
        try {
            //处理Prepare消息 如果在集群情况下，是有可能出现重复消息的，这里演示使用分布式锁
            redisUtil.lock("redis_lock", 90_000L, () -> {
                List<PrepareMessage> prepare = coordinator.getPrepare();
                if (!Objects.isNull(prepare) && prepare.size() > 0) {
                    prepare.forEach(p -> {
                        //可以首先根据p.getBizId()获取业务id进行分类
                        //再根据回查id（这里是用数据库id，可以根据自己的业务场景采用其他方式）查询服务A的该任务是否已经完成，
                        RouteConfig config = mapper.selectByPrimaryKey(Long.valueOf(p.getCheckBackId()));
                        if (Objects.isNull(config)) {
                            log.info("服务A中任务并没有完成，CheckBackId:{}", p);
                            //因为服务A的任务没有完成，所以这次操作就是失败了，可以记录下日志，这时候数据是一致的
                            coordinator.delPrepare(p.getCheckBackId());
                        } else {
                            log.info("服务A中任务已经完成，CheckBackId:{}", p);
                            //服务A的任务已经完成，但是prepare消息没被删除，说明投递到mq失败了，那就继续进行投递或者将任务回滚
                            try {
                                //如果要任务回滚，可以按照业务自行回滚
                                //如果进行投递,有需要传递信息，则需要重新加上，这里演示继续投递，模拟之前的data
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
