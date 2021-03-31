package com.continual.lab.relationImport;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import org.bson.Document;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Bulk {
    private final Object lock = new Object();
    private final MongoCollection<Document> relationsCol;
    private final int size = 5000;
    private Document[] bulk = new Document[size];
    private int i = 0;

    public Bulk(MongoCollection<Document> relationsCol) {
        this.relationsCol = relationsCol;
    }

    public void add(Document document) {
        synchronized (lock) {
            if (i == 5000) {
                write();
            }
            bulk[i] = document;
            i++;
        }
    }

    public void write() {
        relationsCol.bulkWrite(Stream.of(bulk).filter(Objects::nonNull).map(InsertOneModel::new).collect(Collectors.toList()));
        bulk = new Document[size];
        i = 0;
    }


}
