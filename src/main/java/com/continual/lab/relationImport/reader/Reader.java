package com.continual.lab.relationImport.reader;

import de.topobyte.osm4j.core.model.iface.EntityContainer;

import java.util.stream.Stream;

public interface Reader {
    Stream<EntityContainer> read();
}
