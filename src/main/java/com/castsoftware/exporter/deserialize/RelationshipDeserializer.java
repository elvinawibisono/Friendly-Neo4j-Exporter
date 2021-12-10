package com.castsoftware.exporter.deserialize;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.database.Neo4jAlUtils;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.utils.Shared;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RelationshipDeserializer {

	/**
	 * Create a node from a list of values
	 * @param neo4jAl Neo4j Access Layer
	 * @param headers Headers
	 * @param values Values
	 * @return
	 */
	public static Relationship mergeRelationship(Neo4jAl neo4jAl, List<String> headers, List<String> values) throws Neo4jQueryException, Exception {
		// Verify the map
		Map<String, String> zipped = Neo4jTypeMapper.zip(headers, values);

		// Get exporter parameters
		String start = Neo4jTypeMapper.verifyMap(zipped, Shared.RELATIONSHIP_START);
		String end = Neo4jTypeMapper.verifyMap(zipped, Shared.RELATIONSHIP_END);
		String sType = Neo4jTypeMapper.verifyMap(zipped, Shared.RELATIONSHIP_TYPE);

		// Transform
		Long lStart = Long.parseLong(start);
		Long lEnd = Long.parseLong(end);

		// Get the list of non null properties
		Map<String, Object> properties = new HashMap<>();
		Object neoType;
		for(String h : headers) { // Get the map of neo4j type
			if(h.equals(Shared.RELATIONSHIP_START) || h.equals(Shared.RELATIONSHIP_END) || h.equals(Shared.RELATIONSHIP_TYPE)) continue; // Skip defaults

			neoType = Neo4jTypeMapper.getNeo4jType(zipped, h);
			if( neoType != null ) properties.put(h, neoType);
		}

		// Get existing node or create
		Relationship r;
		Optional<Relationship> relationshipOptional = Neo4jAlUtils.getRelationship(neo4jAl, sType, lStart, lEnd);
		if(relationshipOptional.isEmpty()) r = Neo4jAlUtils.createRelationship(neo4jAl, sType, lStart, lEnd, properties);
		else r = relationshipOptional.get();

		return r;
	}


}
