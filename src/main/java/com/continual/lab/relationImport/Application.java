package com.continual.lab.relationImport;

import com.continual.lab.relationImport.reader.AsyncBlobReader;
import com.continual.lab.relationImport.reader.Reader;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.topobyte.osm4j.core.model.iface.EntityContainer;
import de.topobyte.osm4j.core.model.iface.OsmEntity;
import de.topobyte.osm4j.core.model.util.OsmModelUtil;
import de.topobyte.osm4j.pbf.raf.PbfFile;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

public class Application {

    public static void main(String[] args) {
        try {
            MongoCredential credential = MongoCredential.createCredential("admin", "admin", "qwe123!@#".toCharArray());
            MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27018), credential, MongoClientOptions.builder().build());
            MongoDatabase db = mongoClient.getDatabase("mea_vf_branch");
            MongoCollection<Document> relationsCol = db.getCollection("test_relations_new_3");
            relationsCol.drop();
            relationsCol.createIndex(new Document("id", 1));
            File file = new File("/home/andrey/Downloads/us-latest.osm.pbf");
            PbfFile pbfFile = new PbfFile(file);
            pbfFile.buildBlockIndex();
            Mapper mapper = new Mapper();
            Reader reader = new AsyncBlobReader(pbfFile, 0, pbfFile.getNumberOfDataBlocks() - 1, mapper, relationsCol);
            Instant before = Instant.now();

            reader.read();

            System.out.println(Instant.now().getEpochSecond() - before.getEpochSecond());
            System.out.println("finished");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
