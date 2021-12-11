package com.castsoftware.exporter.deserialize;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.database.Neo4jAlUtils;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.utils.Shared;

import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NodeDeserializer {



	/**
	 * Create a node from a list of values
	 * @param neo4jAl Neo4j Access Layer
	 * @param headers Headers
	 * @param values Values
	 * @return
	 */
	public static Node mergeNode(Neo4jAl neo4jAl, List<String> headers, List<String> values) throws Neo4jQueryException, Exception {
		// Verify the map
		Map<String, String> zipped = Neo4jTypeMapper.zip(headers, values);

		// Get exporter parameters
		Object id = Neo4jTypeMapper.verifyMap(zipped, Shared.NODE_ID);
		Object labels = Neo4jTypeMapper.verifyMap(zipped, Shared.NODE_LABELS);
		// Transform
		Long sId = Long.parseLong(id.toString());
		List<String> sLabels = Neo4jTypeMapper.getAsStringList(labels);

		// Get the list of non null properties
		Map<String, Object> properties = new HashMap<>();
		Object neoType;
		for(String h : headers) { // Get the map of neo4j type
			if(h.equals(Shared.NODE_ID) || h.equals(Shared.NODE_LABELS)) continue; // Skip defaults

			neoType = Neo4jTypeMapper.getNeo4jType(zipped, h);
			if( neoType != null ) properties.put(h, neoType);
		}

		// Get existing node or create
		Node n;
		Optional<Node> nodeOptional = Neo4jAlUtils.getNode(neo4jAl, sLabels, properties);
		if(nodeOptional.isEmpty()) n = Neo4jAlUtils.createNode(neo4jAl, sLabels, properties);
		else n = nodeOptional.get();

		n.setProperty(Shared.TEMP_ID_VALUE, sId);
		return n;
	}


}
