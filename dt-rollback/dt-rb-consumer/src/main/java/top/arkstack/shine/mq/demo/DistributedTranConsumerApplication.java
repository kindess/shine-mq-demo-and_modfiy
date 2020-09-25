package top.arkstack.shine.mq.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * @author 7le
 * @version 1.0.0
 */
@SpringBootApplication
@EnableTransactionManagement
public class DistributedTranConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributedTranConsumerApplication.class, args);
    }
}
