package com.alibaba.middleware.race.mom.test;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.DefaultConsumer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public class TestConsumer {
    public static void main(String[] args) {
        DefaultConsumer consumer = new DefaultConsumer();
        consumer.setGroupId("consumer group");
        consumer.subscribe("topic", null, new MessageListener() {
            @Override
            public ConsumeResult onMessage(Message message) {
                return ConsumeResult.successResult("this is info");
            }
        });
        consumer.start();
    }

}
