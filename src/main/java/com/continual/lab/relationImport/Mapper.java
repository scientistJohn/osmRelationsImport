package com.continual.lab.relationImport;

import de.topobyte.osm4j.core.model.iface.EntityContainer;
import de.topobyte.osm4j.core.model.iface.OsmEntity;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

public class Mapper {
    public Document map(EntityContainer container) {
        OsmEntity e = container.getEntity();
        Map<String, Object> tags = new HashMap<>();
        for (int i = 0; i < e.getNumberOfTags(); i++) {
            tags.put(e.getTag(i).getKey().replaceAll("[^a-zA-Z]", ""), e.getTag(i).getValue());
        }
        Document document = new Document();
        document.put("id", e.getId());
        document.put("tags", tags);
        document.put("entityType", container.getType().name());
        return document;
    }
}
