package com.alibaba.middleware.race.momtest;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.Consumer;
import com.alibaba.middleware.race.mom.DefaultConsumer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;

public class ConusmerTest {
	public static void main(String[] args) {
		for (int i = 0; i < 10; i++) {
			System.out.println(System.nanoTime()/10000);
		}


	}
}
