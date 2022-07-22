package com.castsoftware.exporter.database;

import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.utils.Shared;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;

import javax.swing.text.html.Option;
import java.util.*;

public class Neo4jAlUtils {
	

//	private Neo4jAl neo4jAl;

	/**
	 * Format the string of labels
	 * 
	 * @param labels List of labels
	 * @return The String with
	 */
	private static String formatLabels(List<String> labels) {
		List<String> sb = new ArrayList<>();
		for (String l : labels) {
			sb.add(String.format("`%s`", l));
		}



		if (sb.size() == 0)
			return "";
		else
			return ":" + String.join(":", sb);
	}

	/* 
	private static formatDefLabels(List<String>labels, List<String> headers){

	}
	*/

	/**
	 * Format the where clause to have a string with
	 * 
	 * @param properties Properties of the object
	 * @param o          Label of the object concerned by the parameters
	 * @return
	 */
	private static String formatWhere(Map<String, Object> properties, String o) {
		List<String> sb = new ArrayList<>();
		for (Map.Entry<String, Object> l : properties.entrySet()) {
			sb.add(String.format("%s.%s=$%s", o, l.getKey(), l.getKey()));
		}
		if (sb.size() == 0)
			return " ";
		else
			return " WHERE " + String.join(" AND ", sb);
	}


	/**
	 * Find a node in the database based on its labels and properties
	 * 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param labels     Labels of the node
	 * @param properties Properties
	 * @return
	 */
	public static Optional<Node> getNode(Neo4jAl neo4jAl, List<String> labels, Map<String, Object> properties)
			throws Neo4jQueryException {

		String sLabels = formatLabels(labels);
		neo4jAl.info(sLabels); 
		String sWhereClause = formatWhere(properties, "o");
		neo4jAl.info(sWhereClause);
		String req = String.format("MATCH (o%1$s) %2$s RETURN o as node", sLabels, sWhereClause);
		neo4jAl.info(req);

		try {
			Result res = neo4jAl.executeQuery(req, properties);
			if (res.hasNext())
				return Optional.ofNullable((Node) res.next().get("node"));
			else
				return Optional.empty();
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the node. Request : %s", req), e);
			throw new Neo4jQueryException("Failed to get the node.", e, "NEO4JUTILS");
		}
	}

	/**
	 * [modified]
	 * Find the node if exist and create a node if not exist 
	 * @param neo4jAl
	 * @param headers
	 * @return
	 * @throws Neo4jQueryException
	 */

	public static Optional<Node>getNodeType(Neo4jAl neo4jAl, String headers) throws Neo4jQueryException{
		String req = String.format("MERGE (o: `%s` {%s: '%s'})  RETURN o as node", Shared.DEF_NODE_LABELS, Shared.NODE_PROPERTY_TYPE, headers ); 

		neo4jAl.info(req);
		try{
			Result res = neo4jAl.executeQuery(req); 

			return  Optional.ofNullable((Node) res.next().get("node"));

			/* 
			if(res.hasNext()){

				return  Optional.ofNullable((Node) res.next().get("node"));

			}		
		
			else{

				return Optional.empty(); 
			}

			*/
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the node. Request : %s", req), e);
			throw new Neo4jQueryException("Failed to get the node.", e, "NEO4JUTILS");
		}
		
	}

	
	public static Map<String, List<Node>> sortNodesByLabel(Neo4jAl neo4jAl, List<Long> ids) throws Neo4jQueryException {
		Map<String, List<Node>> returnMap = new HashMap<>();
		String req = "MATCH (o) WHERE ID(o) IN $idList " +
				"RETURN DISTINCT o as node, LABELS(o)[0] as label";

		try {
			Map<String, Object> records;
			Result res = neo4jAl.executeQuery(req, Map.of("idList", ids));
			while (res.hasNext()) {
				records = res.next();
				returnMap.putIfAbsent((String) records.get("label"), new ArrayList<>());
				returnMap.get((String) records.get("label")).add((Node) records.get("node"));
			}

			return returnMap;
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to build the node map. Request : %s", req), e);
			throw new Neo4jQueryException("Failed to build the node map.", e, "NEO4JUTILS");
		}
	}



	/**
	 * Create a node based on a list of labels and properties
	 * 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param labels     Labels of the node
	 * @param properties Properties
	 * @return
	 * @throws Neo4jQueryException
	 */
	public static Node createNode(Neo4jAl neo4jAl, List<String> labels, Map<String, Object> properties)
			throws Neo4jQueryException {
		try {
			Node n = neo4jAl.createNode();
			labels.forEach(x -> n.addLabel(Label.label(x))); // Add the labels
			for (Map.Entry<String, Object> l : properties.entrySet()) {
				n.setProperty(l.getKey(), l.getValue());
			}

			return n;
		} catch (Exception e) {
			neo4jAl.error("Failed to create the node.", e);
			throw new Neo4jQueryException("Failed to create the node.", e, "NEO4JUTILS");
		}
	}


	/**
	 * Get a relationship
	 * 
	 * @param neo4jAl Neo4j Access Layer
	 * @param type    Type of the relationship
	 * @param start   Start ID
	 * @param end     End ID
	 * @return An optional of the relationship
	 * @throws Neo4jQueryException
	 */
	public static Optional<Relationship> getRelationship(Neo4jAl neo4jAl, String type, Long start, Long end)
			throws Neo4jQueryException {
		String req = String.format("MATCH (a)-[r:`%1$s`]-(b) " +
				"WHERE o.%2$s=$start AND o.%2$s=$end " +
				"RETURN r as relationship", type, Shared.TEMP_ID_VALUE);

		neo4jAl.info(req); 

		try {
			Result res = neo4jAl.executeQuery(req, Map.of("start", start, "end", end));
			if (res.hasNext())
				return Optional.ofNullable((Relationship) res.next().get("relationship"));
			else
				return Optional.empty();
		} catch (Exception e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}


	
	
	/*
	 * Get a relationship
	 * 
	 * @param neo4jAl Neo4j Access Layer
	 * @param start   Start ID
	 * @param end     End ID
	 * @param weight 
	 * @return An optional of the relationship
	 * @throws Neo4jQueryException
	 */
	/* 
	public static Optional<Relationship> getRelationshipType(Neo4jAl neo4jAl, List<String> start, List<String> end , Double weight)
			throws Neo4jQueryException {
		String req = String.format("MATCH (a : `%s` {`%s` : '%s'})-[r:`%s`]->(b:`%s` {`%s` : '%s'}) " +
				"RETURN r as relationship", Shared.DEF_NODE_LABELS, Shared.NODE_PROPERTY_TYPE, start, Shared.RELATIONSHIP_PROP_TYPE, 
				 Shared.DEF_NODE_LABELS, Shared.NODE_PROPERTY_TYPE,end);
		
		neo4jAl.info(req); 

		try {
			Result res = neo4jAl.executeQuery(req);
			if (!res.hasNext()){

				return Optional.empty(); 
			}
			else{
				return Optional.ofNullable((Relationship) res.next().get("relationship"));
			}
			/* 
			if (res.hasNext())
				return Optional.ofNullable((Relationship) res.next().get("relationship"));
			else
				return Optional.empty();

				
		} catch (Exception e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}

	*/

	
	/**
	 * Get a relationship
	 * 
	 * @param neo4jAl Neo4j Access Layer
	 * @param start   Start ID
	 * @param end     End ID
	 * @param weight 
	 * @return An optional of the relationship
	 * @throws Neo4jQueryException
	 */
	public static Optional<Relationship> getRelationshipType(Neo4jAl neo4jAl, String start, String end , Double weight)
			throws Neo4jQueryException {
		String req = String.format("MATCH (a : `%s` {`%s` : '%s'})-[r:`%s`]->(b:`%s` {`%s` : '%s'})  SET r.`%s` = %f "  +
				"RETURN r as relationship", Shared.DEF_NODE_LABELS, Shared.NODE_PROPERTY_TYPE, start, Shared.RELATIONSHIP_PROP_TYPE,
				 Shared.DEF_NODE_LABELS, Shared.NODE_PROPERTY_TYPE,end, Shared.RELATIONSHIP_PROP_VALUE, weight);
		
		neo4jAl.info(req); 

		try {
			Result res = neo4jAl.executeQuery(req);
		
			if (res.hasNext())
				return Optional.ofNullable((Relationship) res.next().get("relationship"));
			else
				return Optional.empty();

				
		} catch (Exception e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}


	/**
	 * [modified]
	 * Create a relationship
	 * 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param start      Start ID
	 * @param end        End id
	 * @param weight 
	 * @return
	 */
	public static Relationship createRelationshipType(Neo4jAl neo4jAl, String start, String end, Double weight) throws Neo4jQueryException {
		// Create the relationship
		String req = String.format("MATCH (a : `%s` {`%s` : '%s'}), (b:`%s` {`%s` : '%s'})" +
				"MERGE (a)-[r:`%s`{`%s`: %f}]->(b) " +
				"RETURN r as relationship ", Shared.DEF_NODE_LABELS, Shared.NODE_PROPERTY_TYPE, start, Shared.DEF_NODE_LABELS, Shared.NODE_PROPERTY_TYPE,
				end, Shared.RELATIONSHIP_PROP_TYPE, Shared.RELATIONSHIP_PROP_VALUE, weight );

		neo4jAl.info(req); 
		neo4jAl.info(String.format("weight2: %s", weight.toString())); 

		try {

			Result res; 

			Relationship rel = null; 

			neo4jAl.info(String.format("weight: %s", weight.toString()));

			
			if(weight != Shared.DEF_WEIGHT){

				res = neo4jAl.executeQuery(req);

				if(!res.hasNext()){

					throw new Error("Failed to create the relationship. No return resulted"); 
				}

				rel = (Relationship) res.next().get("relationship"); 

				neo4jAl.info(rel.toString()); 

			}

			else {
				
				rel = null; 
				
			}

			return rel; 

			

			/* 
			Result res = neo4jAl.executeQuery(req);
			if (!res.hasNext())
				throw new Error("Failed to create the relationship. No return resulted.");

			Relationship rel = (Relationship) res.next().get("relationship");
			
			/* 
			// Apply properties
			for (Map.Entry<String, Object> l : properties.entrySet()) {
				rel.setProperty(l.getKey(), l.getValue());
			}
			

			return rel;
			*/

		} catch (Exception | Neo4jQueryException e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}

 

	/**
	 * [modified]
	 * Create a relationship
	 * 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param type       Type of the relationship
	 * @param start      Start ID
	 * @param end        End id
	 * @param properties List of properties
	 * @return
	 */
	public static Relationship createRelationship(Neo4jAl neo4jAl, String type, Long start, Long end,
			Map<String, Object> properties) throws Neo4jQueryException {
		// Create the relationship
		String req = String.format("MATCH (a), (b) " +
				"WHERE o.%2$s=$start AND o.%2$s=$end " +
				"MERGE (a)-[r:`%1$s`]-(b) " +
				"RETURN r as relationship ", type, Shared.TEMP_ID_VALUE);
		
		neo4jAl.info(req); 

		try {
			Result res = neo4jAl.executeQuery(req, Map.of("start", start, "end", end));
			if (!res.hasNext())
				throw new Error("Failed to create the relationship. No return resulted.");

			Relationship rel = (Relationship) res.next().get("relationship");

			// Apply properties
			for (Map.Entry<String, Object> l : properties.entrySet()) {
				rel.setProperty(l.getKey(), l.getValue());
			}

			return rel;

		} catch (Exception | Neo4jQueryException e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}

}
