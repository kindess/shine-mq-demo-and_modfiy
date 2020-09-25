package top.arkstack.shine.mq.demo.simple.producer;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import top.arkstack.shine.mq.RabbitmqFactory;
import javax.annotation.PostConstruct;

/**
 * @author 7le
 * @version 1.0.0
 */
@Component
public class Producer {

    @Autowired
    RabbitmqFactory factory;

    @Autowired
    DistributedTran distributedTran;

    /**
     * @PostConstruct和@PreDestroy，这两个注解被用来修饰 ①非静态的 ②void（）方法
     * @throws Exception
     */
    @PostConstruct
    public void test() throws Exception {
        //服务A 执行任务
        for (int i = 0; i < 1; i++) {
            // 等同于service层业务
            distributedTran.transaction();
        }
    }
}
