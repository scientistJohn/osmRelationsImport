package com.continual.lab.relationImport.reader;

import de.topobyte.osm4j.core.model.iface.EntityContainer;
import de.topobyte.osm4j.pbf.seq.PbfIterator;

import java.io.IOException;
import java.io.InputStream;

public class StatPbfIterator extends PbfIterator {

    private final InputStream input;

    private int nextCounter;

    StatPbfIterator(InputStream input) {
        super(input, false);
        this.input = input;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = super.hasNext();

        if(!hasNext) {
            try {
                input.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return hasNext;
    }
}
