package com.castsoftware.exporter.utils;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RelationshipsUtils {

	/**
	 * Get the list of properties for a relationship
	 * @param neo4jAl Neo4j Access Layer
	 * @param type Type of the relationship
	 * @return The list of values as object
	 */
	public static List<String> getKeysByType(Neo4jAl neo4jAl, String type) {
		String request = String.format("MATCH ()-[a:`%s`]-() UNWIND keys(a) AS key RETURN collect(distinct key) as keys", type);

		try {
			Result result = neo4jAl.executeQuery(request);
			if(!result.hasNext()) return new ArrayList<>();
			else return ((List<String>) result.next().get("keys")).stream()
					.sorted().collect(Collectors.toList());
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the relationship '%s'", type), e);
			throw new Error("Failed to get relationship's keys");
		}
	}

	/**
	 * Get the list of relationship type for a label
	 * @param neo4jAl Neo4j Access Layer
	 * @param label Label to process
	 * @return The list of values as object
	 */
	public static List<String> getRelationshipTypeForLabel(Neo4jAl neo4jAl, String label) {
		String request = String.format("MATCH (o:`%s`)-[a]-() UNWIND Type(a) AS key RETURN collect(distinct key) as rels", label);

		try {
			Result result = neo4jAl.executeQuery(request);
			if(!result.hasNext()) return new ArrayList<>();
			else return ((List<String>) result.next().get("rels")).stream()
					.sorted().collect(Collectors.toList());
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list relationships for label '%s'", label), e);
			throw new Error("Failed to get the list of relationships");
		}
	}


	/**
	 * Get the list of values for a specific node
	 * @param neo4jAl Neo4j Access Layer
	 * @param n Node to check
	 * @param keys Keys to validate
	 * @return The list of values as objects
	 */
	public static List<Object> getValues(Neo4jAl neo4jAl, Relationship n, List<String> keys) {
		try {
			List<Object> values = new ArrayList<>();
			Object it;
			for (String k : keys) { // Parse the keys
				it = n.hasProperty(k) ? n.getProperty(k) : null;
				values.add(it);
			}
			return values;
		} catch (Exception e) {
			neo4jAl.error(String.format("Failed to extract values from node with id [%d] ", n.getId()), e);
			throw new Error("Failed to extract values from the node.");
		}
	}

	/**
	 * Get the list of relationship attached to the node
	 * @param n Node to process
	 * @param direction Direction of the relationship
	 * @return
	 */
	public static Set<Relationship> getRelationships(Node n, Direction direction) {
		return StreamSupport
				.stream(n.getRelationships(direction).spliterator(), false)
				.collect(Collectors.toSet());
	}

	/**
	 * Get the list of relationship attached to the node
	 * @param n Node to process
	 * @param type Type of the relation
	 * @param direction Direction
	 * @return The set of relationship
	 */
	public static Set<Relationship> getRelationships(Node n, String type, Direction direction) {
		return StreamSupport
				.stream(n.getRelationships(direction, RelationshipType.withName(type)).spliterator(), false)
				.collect(Collectors.toSet());
	}

	/**
	 * Get the list of relationship attached to the node considering the type of the other node
	 * @param n Node to process
	 * @param direction Direction of the relationship
	 * @param toConsider List of node to consider
	 * @return
	 */
	public static Set<Relationship> getRelationships(Node n,  Direction direction, List<String> toConsider) {
		return StreamSupport
				.stream(n.getRelationships(direction).spliterator(), false)
				.filter(x -> {
					// Filter nodes where at least a label is in the list
					for(Label l : x.getOtherNode(n).getLabels()) {
						if(toConsider.contains(l.name())) return true;
					}
					return false;
				})
				.collect(Collectors.toSet());
	}

}
