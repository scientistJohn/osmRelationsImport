package com.continual.lab.relationImport.reader;

import com.continual.lab.relationImport.Application;
import com.continual.lab.relationImport.Bulk;
import com.continual.lab.relationImport.Mapper;
import com.mongodb.client.MongoCollection;
import de.topobyte.osm4j.core.model.iface.EntityContainer;
import de.topobyte.osm4j.pbf.Constants;
import de.topobyte.osm4j.pbf.protobuf.Fileformat;
import de.topobyte.osm4j.pbf.raf.PbfFile;
import de.topobyte.osm4j.pbf.seq.BlockWriter;
import de.topobyte.osm4j.pbf.seq.PbfIterator;
import org.bson.Document;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AsyncBlobReader implements Reader {
    Executor executor = Executors.newCachedThreadPool();
    private final PbfFile pbfFile;
    private final int start;
    private final int end;
    private final Mapper mapper;
    private final MongoCollection<Document> relationsCol;
    private final AtomicBoolean finish = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<Fileformat.Blob> blobs = new ConcurrentLinkedQueue<>();

    public AsyncBlobReader(PbfFile pbfFile,
                           int start,
                           int end,
                           Mapper mapper,
                           MongoCollection<Document> relationsCol) {
        this.pbfFile = pbfFile;
        this.start = start;
        this.end = end;
        this.mapper = mapper;
        this.relationsCol = relationsCol;
    }

    public Stream<EntityContainer> read() {
        CompletableFuture.runAsync(() -> {
            for (int i = start; i <= end; i++) {
                try {
                    blobs.offer(pbfFile.getDataBlob(i));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            finish.set(true);
        }, executor);

        for (int i = 0; i < 10; i++) {
            CompletableFuture.runAsync(this::readBlocks, executor);
        }

        return Stream.empty();
    }

    private void readBlocks() {
        try {
            PipedInputStream inputStream = new PipedInputStream();
            PipedOutputStream outputStream = new PipedOutputStream(inputStream);
            CompletableFuture.runAsync(() -> readBlocks(outputStream), executor);
            PbfIterator iterator = new PbfIterator(inputStream, false);
            Bulk bulk = new Bulk(relationsCol);

            while (!finish.get()) {
                while (iterator.hasNext()) {
                    EntityContainer container = iterator.next();
                    bulk.add(mapper.map(container));
                }
            }
            bulk.write();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void readBlocks(PipedOutputStream outputStream) {
        //System.out.println("Thread started: " + currThread);

        try {
            BlockWriter blockWriter = new BlockWriter(outputStream);
//            while (true) {
//                try {
//                    blockWriter.write(Constants.BLOCK_TYPE_DATA, null, getBlob(start));
//                    break;
//                } catch (Exception e) {
//                    start++;
//                }
//            }
            //System.out.println("Thread: " + currThread + " read: " + start);
            while (!blobs.isEmpty() || !finish.get()) {
                Fileformat.Blob blob = blobs.poll();
                if (blob != null) {
                    blockWriter.write(Constants.BLOCK_TYPE_DATA, null, blob);
                }
            }
            outputStream.close();
            //System.out.println("Thread finished: " + currThread);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
