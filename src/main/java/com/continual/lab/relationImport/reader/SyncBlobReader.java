package com.continual.lab.relationImport.reader;

import de.topobyte.osm4j.core.model.iface.EntityContainer;
import de.topobyte.osm4j.pbf.Constants;
import de.topobyte.osm4j.pbf.protobuf.Fileformat;
import de.topobyte.osm4j.pbf.raf.PbfFile;
import de.topobyte.osm4j.pbf.seq.BlockWriter;
import de.topobyte.osm4j.pbf.seq.PbfIterator;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SyncBlobReader implements Reader {
    Executor executor = Executors.newCachedThreadPool();
    private final PbfFile pbfFile;
    private final int start;
    private final int end;

    public SyncBlobReader(PbfFile pbfFile, int start, int end) {
        this.pbfFile = pbfFile;
        this.start = start;
        this.end = end;
    }

    public Stream<EntityContainer> read() {
        try {
            System.out.println("Start reading blocks");
            PipedInputStream inputStream = new PipedInputStream();
            PipedOutputStream outputStream = new PipedOutputStream(inputStream);
            CompletableFuture.runAsync(() -> {
                        try {
                            BlockWriter blockWriter = new BlockWriter(outputStream);

                            for (int block = start + 1; block < end; block++) {
                                Fileformat.Blob blob = pbfFile.getDataBlob(block);
                                //System.out.println("got blob for block " + block);
                                blockWriter.write(Constants.BLOCK_TYPE_DATA, null, blob);
                                //System.out.println("blob for block has been written" + block);
                            }
                            outputStream.close();
                            System.out.println("closed");
                        } catch (IOException e) {
                            e.printStackTrace();
                            System.out.println("error " + e.getMessage());
                        }
                    },
                    executor);

            PbfIterator iterator = new StatPbfIterator(inputStream);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL), false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
