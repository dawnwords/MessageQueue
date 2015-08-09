package com.alibaba.middleware.race.momtest;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.Consumer;
import com.alibaba.middleware.race.mom.DefaultConsumer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;

public class ConusmerTest {
	public static void main(String[] args) {
		Consumer consumer = new DefaultConsumer();
		//����������id��groupid��ͬ�������ߣ�broker����Ϊͬһ�������߼�Ⱥ��ÿ����Ϣֻ��Ͷ�ݸ���Ⱥ�е�һ̨����
		consumer.setGroupId("CG-test");
		//�����Ĳ�����brokerֻ��Ͷ�� topicΪT-test������area����Ϊus����Ϣ��������
		consumer.subscribe("T-test", "area=us"/*���������Ϊnull���߿մ�����ô��ʾ�������topic�µ�������Ϣ*/, new MessageListener() {

			@Override
			public ConsumeResult onMessage(Message message) {
				assert "T-test".equals(message.getTopic()) && "us".equals(message.getProperty("area"));
				System.out.println("consume success:" + message.getMsgId());
				ConsumeResult result = new ConsumeResult();
				//�������ѽ��������ɹ�����ôbroker����Ͷ��
				result.setStatus(ConsumeStatus.SUCCESS);
				//�������Ϊʧ�ܣ���ôbroker�ᾡ������Ͷ�ݣ�ֱ�����سɹ���
				//result.setStatus(ConsumeStatus.FAIL);
				//����ʧ��Ҫ����ʧ��ԭ��broker���Ի�ȡ�������Ϣ
				//result.setInfo("fail detail or reason");
				return result;
			}
		});
		consumer.start();
	}
}
