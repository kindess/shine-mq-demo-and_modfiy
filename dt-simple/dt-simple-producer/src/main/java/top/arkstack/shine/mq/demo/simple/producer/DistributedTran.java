package top.arkstack.shine.mq.demo.simple.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import top.arkstack.shine.mq.annotation.DistributedTrans;
import top.arkstack.shine.mq.bean.TransferBean;
import top.arkstack.shine.mq.coordinator.Coordinator;
import top.arkstack.shine.mq.demo.simple.dao.RouteConfigMapper;
import top.arkstack.shine.mq.demo.simple.dao.model.RouteConfig;
import top.arkstack.shine.mq.demo.simple.util.SnowflakeIdGenerator;

/**
 * 分布式事务demo 自行配置对应参数
 *
 * Service业务层
 *
 * @author 7le
 * @version 1.0.0
 */
@Component
public class DistributedTran {

    @Autowired
    private Coordinator coordinator;

    @Autowired
    private RouteConfigMapper mapper;

    /**
     * 服务A 的任务
     * <p>
     * coordinator 可以自行实现，或者使用默认提供的
     * 注解@DistributedTrans可以和@Transactional共用，自定义切面与事务切面（默认order优先级最低，最后执行）共用时，
     * 事务切面能不能捕获到自定义切面抛出的异常（运行异常，比如空指针等等）？
     * @Transactional注解默认将自定义切面与事务方法是同一个事务，自定义切面异常，也会导致事务回滚。
     * 可参考 https://blog.csdn.net/wz1159/article/details/97891829?utm_medium=distribute.pc_relevant.none-task-blog-BlogCommendFromMachineLearnPai2-2.channel_param&depth_1-utm_source=distribute.pc_relevant.none-task-blog-BlogCommendFromMachineLearnPai2-2.channel_param
     */
    @DistributedTrans(exchange = "simple_route_config", routeKey = "simple_route_config_key", bizId = "simple_route_config")
    @Transactional(rollbackFor = Exception.class)
    public TransferBean transaction() {
        //simple 不校验服务A的状态 可以不设置Prepare状态
        // 1、生成业务主键checkBackId，用途：回调检查
        Long checkBackId = SnowflakeIdGenerator.getInstance().nextNormalId();
        //执行操作
        RouteConfig routeConfig = new RouteConfig(checkBackId,
                "/shine/simple/**", "spring-mq-simple", null, false, true,
                true, null);
        mapper.insert(routeConfig);
        // 使用DistributedTrans注解，要求必须返回一个TransferBean，TransferBean的data属性就是要发送的消息json
        return new TransferBean(checkBackId.toString(), routeConfig.getPath());
    }
}
