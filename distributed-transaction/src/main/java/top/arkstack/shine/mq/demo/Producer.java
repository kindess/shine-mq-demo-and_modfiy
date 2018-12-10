package top.arkstack.shine.mq.demo;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import top.arkstack.shine.mq.RabbitmqFactory;

import javax.annotation.PostConstruct;

@Component
public class Producer {

    @Autowired
    RabbitmqFactory factory;

    @Autowired
    DefaultDistributedTran defaultDistributedTran;

    @Autowired
    DistributedTran distributedTran;

    @PostConstruct
    public void test() throws Exception {
        //服务A 执行任务
        for (int i = 0; i < 10; i++) {
            defaultDistributedTran.transaction();
            distributedTran.transaction();
        }
    }
}
