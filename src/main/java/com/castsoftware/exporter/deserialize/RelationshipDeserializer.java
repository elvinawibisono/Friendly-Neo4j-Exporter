package com.castsoftware.exporter.deserialize;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.database.Neo4jAlUtils;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.utils.Shared;

import org.neo4j.cypher.internal.ir.CreateRelationship;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import javax.swing.text.html.Option;

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


	/**
	 * Create a node from a list of values
	 * @param neo4jAl Neo4j Access Layer
	 * @param headers Headers
	 * @param values Values
	 * @return
	 */
	public static Relationship mergeRelationshipType(Neo4jAl neo4jAl, List<String> headers, List<String> values) throws Neo4jQueryException, Exception {

		List<String>end = new ArrayList<>(); 
		List<String>start = new ArrayList<>(); 
		//List<Double>weight = new ArrayList<>();  
		String source; 
		String sEnd; 
		Double weight; 
		neo4jAl.info(headers.toString()); 

		Relationship r = null; 

				
		for (int j = 1; j<headers.size(); j++){

			neo4jAl.info(values.toString());

			start = new ArrayList<>(); 
			start.add(values.get(0).toString());	
			source = String.join(",", start); 

			end = new ArrayList<>(); 
			end.add(headers.get(j).toString()); 
			sEnd = String.join(",", end); 

			/* 
			weight = new ArrayList<>(); 
			weight.add(Double.valueOf(values.get(j))); 
			//neo4jAl.info(String.valueOf(weight));
			*/
			weight = Double.valueOf(values.get(j)); 

			
			neo4jAl.info(String.format("start : `%s`" , String.join(",", start.toString())));  

			neo4jAl.info(String.format("end : `%s`" , String.join(",", end.toString()))); 

			neo4jAl.info(String.valueOf(weight));

			neo4jAl.info(String.valueOf(headers.size())); 

			neo4jAl.info(String.valueOf(values.size())); 
					
					
				/* 
				  If the two nodes realtionship exist , get the relationship 
				  If the two nodes have relationship , but def weight or diffeent weight (update it)
				  if the two nodes does not have relationship and weight is def values, continue 
				  if the two nodes does not have a relationship and weight is not def_ weight, create a relationship -> do it when three above is working 

				  */
				  
				
			// Relationship r; 
			Optional<Relationship> relationshipOptional = Neo4jAlUtils.getRelationshipType(neo4jAl, source, sEnd, weight);

			neo4jAl.info(relationshipOptional.toString()); 
			neo4jAl.info(String.format("weight1: %s", weight)); 

			neo4jAl.info(String.format("hello: %s", Optional.empty()));

			if(relationshipOptional.isPresent()){

				r = relationshipOptional.get(); 
			}


			neo4jAl.info(String.format("weight you: %s", weight)); 
			neo4jAl.info(String.format("def weight: %s", Shared.DEF_WEIGHT)); 

	
			if(Optional.empty().toString().equals("Optional.empty")){
				neo4jAl.info(String.format("boolean 1: %s", (Double.valueOf(weight) == Shared.DEF_WEIGHT))); 
				
				neo4jAl.info(String.format("boolean 2: %s", (weight == Shared.DEF_WEIGHT)));

				//neo4jAl.info(String.format("boolean 3: %s", (weight.toStriShared.DEF_WEIGHT))));

				if (String.valueOf(weight).equals(String.valueOf(Shared.DEF_WEIGHT))) {
						
					neo4jAl.info(String.format("weight in: %s ", weight.toString())); 
					r = null;					
				}
				else {
					neo4jAl.info(String.format("weight out: %s ", weight.toString())); 

					r = Neo4jAlUtils.createRelationshipType(neo4jAl, source, sEnd, weight); 
		
				}
				

			//	continue; 
			}
		/* 	else if(Optional.empty().toString().equals("Optional.empty") && weight != Shared.DEF_WEIGHT){

				neo4jAl.info(String.format("weight out: %s ", weight.toString())); 

				r = Neo4jAlUtils.createRelationshipType(neo4jAl, source, sEnd, weight); 
			}

*/
			/* 
			else if(weight != Shared.DEF_WEIGHT && relationshipOptional.isEmpty()){

				neo4jAl.info("no weight value , have relationship"); 
				//r = null; 
			}

			*/ 

			

			/* 
			else if(relationshipOptional.isEmpty() && weight != Shared.DEF_WEIGHT){

				r = Neo4jAlUtils.createRelationshipType(neo4jAl, source, sEnd, weight); 
			}

			else if (relationshipOptional.isEmpty()){

				r = null; 
			}
			*/
				
			/* 	
			if(relationshipOptional.isEmpty() && weight== Shared.DEF_WEIGHT){ 

				r = null; 
				  
			}
			
			//if relationship is not fond but
			if(relationshipOptional.isEmpty() && weight != Shared.DEF_WEIGHT){

			


			}

			*/ 

			/* 	  
			else{
				
				
				r = relationshipOptional.get();



			} 
			*/

				  
				
				// r = relationshipOptional.get();
				
				
		}

		return r; 



		/* 

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

		//if realtionship exist and weight is DEF_weight -> update/ create the relationship 
		//if relationship does not exist and weight is DEF_weight -> continue
		// if relationship does not exist  and weight is not DEF_weight -> create a relationship 
		Relationship r;
		Optional<Relationship> relationshipOptional = Neo4jAlUtils.getRelationship(neo4jAl, sType, lStart, lEnd);
		if(relationshipOptional.isEmpty()) r = Neo4jAlUtils.createRelationship(neo4jAl, sType, lStart, lEnd, properties);
		else r = relationshipOptional.get();

		return r;

		*/ 
	}






		



}
