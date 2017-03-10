package org.jandy.queue.core;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.SoftReferenceObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * Publishes messages to the queue attempting retry on any errors
 */
public class QueueWriter {

    private static final Logger log = LoggerFactory.getLogger(QueueWriter.class);

    private final ObjectPool<Pooled> pool;

    public QueueWriter(Connection connection, String queueName) {
        // Use a SoftReferenceObjectPool for ease of configuration but may get slightly better performance using
        // a combination of GenericObjectPool and EvictionPolicy to allow us to batch commits
        Queue queue = ActiveMQJMSClient.createQueue(queueName);
        this.pool = new SoftReferenceObjectPool<>(new PooledObjectFactory(connection, queue));
    }

    public void write(String request) {
        Pooled pooled = null;
        try {
            pooled = pool.borrowObject();
            MessageProducer producer = pooled.producer;
            producer.send(pooled.session.createTextMessage(request));
        } catch (Exception e) {
            log.error("Failed writing message to queue: {}", request, e);
        } finally {
            if (pooled != null) try {
                pool.returnObject(pooled);
            } catch (Exception e) {
                log.error("Failed returning item to pool: {}", pooled, e);
            }
        }
    }

    /**
     * Holds the references we need to get from a pooled item
     */
    private static class Pooled {
        private final Session session;
        private final MessageProducer producer;

        public Pooled(Session session, MessageProducer producer) {
            this.session = session;
            this.producer = producer;
        }
    }

    /**
     * Allows creation of pooled JMS {@link Session} and {@link MessageProducer} objects
     */
    private static class PooledObjectFactory extends BasePooledObjectFactory<Pooled> {
        private final Connection connection;
        private final Queue queue;

        public PooledObjectFactory(Connection connection, Queue queue) {
            this.connection = connection;
            this.queue = queue;
        }

        @Override
        public Pooled create() throws Exception {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            // remove fields to save message size and cpu on generation of value
            producer.setDisableMessageID(true);
            producer.setDisableMessageTimestamp(true);
            return new Pooled(session, producer);
        }

        @Override
        public PooledObject<Pooled> wrap(Pooled obj) {
            return new DefaultPooledObject<>(obj);
        }

        @Override
        public void destroyObject(PooledObject<Pooled> p) throws Exception {
            p.getObject().producer.close();
            p.getObject().session.close();
        }
    }
}
