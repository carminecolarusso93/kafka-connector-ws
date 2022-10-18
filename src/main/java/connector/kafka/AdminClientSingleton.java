package connector.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;

public class AdminClientSingleton {

    private static AdminClient admin = null;
    private static AdminClientSingleton adminSin = null;
    private static String bootstrapServer = "kafka.promenade-lyon.svc.cluster.local:9092";

    private AdminClientSingleton() {
    };

    public static synchronized AdminClientSingleton getAdminClient() {
        if (admin == null && adminSin == null) {
            // creazione admin per la rimozione consumer group
            final Properties propertiesAdm = new Properties();
            propertiesAdm.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            propertiesAdm.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
            propertiesAdm.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
            admin = AdminClient.create(propertiesAdm);
            adminSin = new AdminClientSingleton();
        }
        return adminSin;
    }

    public synchronized void closeConsumerGroup(String id) {

        if (admin == null)
            return;

        System.out.println("DEBUG: eliminazione consumer group: " + id);

        // rimozione consumer group
        DeleteConsumerGroupsResult deleteConsumerGroupsResult = admin.deleteConsumerGroups(Arrays.asList(id));

        KafkaFuture<Void> resultFuture = deleteConsumerGroupsResult.all();
        try {
            resultFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

}
