package database.mongo;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoConnectionManager {

    private final MongoClient mongo;

    public MongoConnectionManager() {
        String hostname = "mongodb1.promenade-lyon.svc.cluster.local";
        String port = "27017";
        String connectionString = "mongodb://" + hostname + ":" + port;
        this.mongo = MongoClients.create(connectionString);
    }

    public MongoClient getClient() {
        return mongo;
    }

    public void close() {
        mongo.close();
    }

    public MongoDatabase getDatabase() {
        CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        return this.mongo.getDatabase("promenade").withCodecRegistry(pojoCodecRegistry);
    }

    public <T> MongoCollection<T> getCollection(String collection, Class<T> documentClass) {
        if (collection == null)
            return this.getDatabase().getCollection("areas", documentClass);
        else
            return this.getDatabase().getCollection(collection, documentClass);
    }
}
