package org.jandy.queue.core;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;

import static com.google.common.base.Throwables.propagate;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.activemq.artemis.api.core.SimpleString.toSimpleString;
import static org.apache.activemq.artemis.api.core.management.ResourceNames.JMS_QUEUE;
import static org.apache.activemq.artemis.api.jms.ActiveMQJMSClient.createConnectionFactoryWithoutHA;

/**
 * Encapsulates the embedded ActiveMQ Artemis server allowing it to be started and stopped. It
 * is the entry point for connections and queue creation. At typical usage is to start the server,
 * create a queue, create a queue producer ({@link QueueWriter}) then create a queue consumer
 * ({@link QueueBatchReader})
 */
public class QueueServer {

    private static final int INFINITE_ATTEMPTS = -1;

    private final EmbeddedJMS server;

    private QueueConnection connection;

    /**
     * Constructs a server with default options
     *
     * @param filesLocation the path to the folder where queue related files will be created
     */
    public QueueServer(String filesLocation) {
        this(createConfig(filesLocation));
    }

    /**
     * Constructs a server with the specified configuration
     */
    public QueueServer(Configuration config) {
        server = new EmbeddedJMS();
        server.setConfiguration(config);
        server.setJmsConfiguration(new JMSConfigurationImpl());
    }

    private static Configuration createConfig(String filesLocation) {
        Configuration config = new ConfigurationImpl();
        config.setSecurityEnabled(false);
        config.setJournalType(JournalType.NIO);
        config.setBindingsDirectory(filesLocation);
        config.setLargeMessagesDirectory(filesLocation);
        config.setJournalDirectory(filesLocation);
        config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
        config.setJournalSyncNonTransactional(false);
        config.setJournalSyncTransactional(false);
        config.setGracefulShutdownEnabled(true);
        config.setGracefulShutdownTimeout(SECONDS.toMillis(30));

        AddressSettings redelivery = new AddressSettings()
            .setAutoCreateJmsQueues(false)
            .setRedeliveryDelay(SECONDS.toMillis(1))
            .setRedeliveryMultiplier(1.5)
            .setMaxRedeliveryDelay(SECONDS.toMillis(30))
            .setMaxDeliveryAttempts(INFINITE_ATTEMPTS);
        config.addAddressesSetting("jms.queue.#", redelivery);
        return config;
    }

    public QueueServer start() {
        try {
            server.start();
        } catch (Exception e) {
            propagate(e);
        }
        return this;
    }

    public void stop() {
        try {
            if (connection != null) {
                connection.close();
            }
            server.stop();
        } catch (Exception e) {
            propagate(e);
        }
    }

    public QueueConnection getConnection() {
        if (connection == null) {
            connection = createConnection();
            try {
                connection.start();
            } catch (JMSException e) {
                propagate(e);
            }
        }
        return connection;
    }

    /**
     * Creates a connection, it should be reused when creating producers and consumers etc.
     */
    private QueueConnection createConnection() {
        TransportConfiguration transport = new TransportConfiguration(InVMConnectorFactory.class.getName());
        ActiveMQConnectionFactory cf = createConnectionFactoryWithoutHA(JMSFactoryType.CF, transport);
        cf.setBlockOnDurableSend(false);
        cf.setBlockOnNonDurableSend(false);
        try {
            return cf.createQueueConnection();
        } catch (JMSException e) {
            throw propagate(e);
        }
    }

    public long getQueueSize(Queue queue) {
        try {
            SimpleString resource = toSimpleString(format("%s%s", JMS_QUEUE, queue.getQueueName()));
            Binding bindings = server.getActiveMQServer().getPostOffice().getBinding(resource);
            QueueBinding queueBinding = (QueueBinding) bindings;
            return queueBinding.getQueue().getMessageCount();
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    public void createQueue(String queueName) {
        try {
            server.getJMSServerManager().createQueue(false, queueName, null, true);
        } catch (Exception e) {
            propagate(e);
        }
    }
}
