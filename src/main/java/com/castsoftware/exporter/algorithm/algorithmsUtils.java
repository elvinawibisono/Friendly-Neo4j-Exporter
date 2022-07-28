package com.castsoftware.exporter.algorithm;

import com.castsoftware.exporter.config.getConfigValues;
import com.castsoftware.exporter.csv.Formatter;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Relationship;


import java.lang.reflect.Array;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.*;
import java.util.stream.Collectors;


public class algorithmsUtils {

    private static final String NODE_PROP_TYPE = getConfigValues.Property.NODE_PROP_TYPE.toString();// name
	private static final String RELATIONSHIP_PROP_VALUE = getConfigValues.Property.RELATIONSHP_PROP_VALUE.toString();
	private static final String RELATIONSHIP_PROP_TYPE = getConfigValues.Property.RELATIONSHIP_PROP_TYPE.toString(); 
	private static final String NODE_LABELS = getConfigValues.Property.NODE_LABELS.toString();
    
    
    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @return
     */
    public static String graphName(Neo4jAl neo4jAl, String node_label, String rels_label) {

        neo4jAl.info(node_label); 
        neo4jAl.info(rels_label); 

        String req = String.format("CALL gds.graph.project('myGraph','%s','%s') YIELD graphName as name", node_label, rels_label);
        
        neo4jAl.info(req);

        try{

            Result result = neo4jAl.executeQuery(req); 

           // List<String>res = new ArrayList<>();

            if(!result.hasNext()){

                return new String();

            } 
			else {

                String output = String.valueOf(result.next().get("name"));

                neo4jAl.info(output);


                return output; 
            }

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		}


    }
    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @return
     */
    public static Map<String,String> louvainAlgo(Neo4jAl neo4jAl, String node_label, String rels_label){

        String graphName = graphName(neo4jAl, node_label, rels_label); 
        neo4jAl.info(graphName);

        //String graphName =  String.join(",",graph); 

       // String req = String.format("CALL gds.louvain.stream('%s') YIELD nodeId, communityId, intermediateCommunityIds RETURN gds.util.asNode(nodeId).name AS name, communityId, intermediateCommunityIds", graphName); 
       String req = String.format("CALL gds.louvain.stream('%s') YIELD nodeId, communityId RETURN gds.util.asNode(nodeId).name AS name, communityId ORDER BY name ASC", graphName); 

        neo4jAl.info(req);

        //Map<String,List<String>> output =  new HashMap<>(); 
        Map<String,String>output = new HashMap<>(); 

        try{

            Result result = neo4jAl.executeQuery(req); 

            if(!result.hasNext()){

                return new HashMap<>();

            }

            else{

                output = new HashMap<>(); 

                while(result.hasNext()) {
                    Map<String,Object> r = result.next();
                    output.put((String)r.get("name"), String.valueOf(r.get("communityId")));
 
                }

               // neo4jAl.output(output); 

                return output; 

            }
			

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		} catch (NoSuchElementException e){
            neo4jAl.error(String.format("Null value"), e); 
            throw new Error("Null value", e); 
            

        }

    }
    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @return
     */

    public static Map<String,String> labelPropAlgo(Neo4jAl neo4jAl, String node_label, String rels_label){

        String graphName = graphName(neo4jAl, node_label, rels_label); 
        neo4jAl.info(graphName);

        //String graphName =  String.join(",",graph); 

       // String req = String.format("CALL gds.louvain.stream('%s') YIELD nodeId, communityId, intermediateCommunityIds RETURN gds.util.asNode(nodeId).name AS name, communityId, intermediateCommunityIds", graphName); 
       String req = String.format("CALL gds.labelPropagation.stream('%s') YIELD nodeId, communityId AS community RETURN gds.util.asNode(nodeId).name AS name, community ORDER BY community,name", graphName); 

        neo4jAl.info(req);

        //Map<String,List<String>> output =  new HashMap<>(); 
        Map<String,String>output = new HashMap<>(); 

        try{

            Result result = neo4jAl.executeQuery(req); 

            if(!result.hasNext()){

                return new HashMap<>();

            }

            else{

                output = new HashMap<>(); 

                while(result.hasNext()) {
                    Map<String,Object> r = result.next();
                    output.put((String)r.get("name"), String.valueOf(r.get("community")));
 
                }

               // neo4jAl.output(output); 

                return output; 

            }
			

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		} catch (NoSuchElementException e){
            neo4jAl.error(String.format("Null value"), e); 
            throw new Error("Null value", e); 
            

        }

    }
    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @return
     */
    public static Map<String,String>weaklyConnected(Neo4jAl neo4jAl, String node_label, String rels_label){

        String graphName = graphName(neo4jAl, node_label, rels_label); 
        String req = String.format("CALL gds.wcc.stream('%s') YIELD nodeId, componentId  RETURN gds.util.asNode(nodeId).name AS name, componentId", graphName); 

        neo4jAl.info(req);

        //Map<String,List<String>> output =  new HashMap<>(); 
        Map<String,String>output = new HashMap<>(); 

        try{

            Result result = neo4jAl.executeQuery(req); 

            if(!result.hasNext()){

                return new HashMap<>();

            }

            else{

                output = new HashMap<>(); 

                while(result.hasNext()) {
                    Map<String,Object> r = result.next();
                    output.put((String)r.get("name"), String.valueOf(r.get("componentId")));
 
                }

               // neo4jAl.output(output); 

                return output; 

            }
			
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		} catch (NoSuchElementException e){
            neo4jAl.error(String.format("Null value"), e); 
            throw new Error("Null value", e); 
            

        }

    }

    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @return
     */
    public static String weightGraphName(Neo4jAl neo4jAl, String node_label, String rels_label) {

        neo4jAl.info(node_label); 
        neo4jAl.info(rels_label); 

        String req = String.format("CALL gds.graph.project('myGraph','%s','%s',{ relationshipProperties : 'weight'}) YIELD graphName as name", node_label, rels_label);
        
        neo4jAl.info(req);

        try{

            Result result = neo4jAl.executeQuery(req); 

           // List<String>res = new ArrayList<>();

            if(!result.hasNext()){

                return new String();

            } 
			else {

                String output = String.valueOf(result.next().get("name"));

                neo4jAl.info(output);


                return output; 
            }

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		}


    }

    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @return
     */

    public static Map<String,String>weightLabelProp(Neo4jAl neo4jAl, String node_label, String rels_label){

        String graphName = weightGraphName(neo4jAl, node_label, rels_label); 
        String req = String.format("CALL gds.labelPropagation.stream('%s', {relationshipWeightProperty: 'weight'}) YIELD nodeId, communityId AS community RETURN gds.util.asNode(nodeId).name AS name, community ORDER BY community,name", graphName); 

        neo4jAl.info(req);

        //Map<String,List<String>> output =  new HashMap<>(); 
        Map<String,String>output = new HashMap<>(); 

        try{

            Result result = neo4jAl.executeQuery(req); 

            if(!result.hasNext()){

                return new HashMap<>();

            }

            else{

                output = new HashMap<>(); 

                while(result.hasNext()) {
                    Map<String,Object> r = result.next();
                    output.put((String)r.get("name"), String.valueOf(r.get("community")));
 
                }

               // neo4jAl.output(output); 

                return output; 

            }
			
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		} catch (NoSuchElementException e){
            neo4jAl.error(String.format("Null value"), e); 
            throw new Error("Null value", e); 
            

        }

    }

    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param name
     * @param comId
     * @return
     */
    public static String setProp(Neo4jAl neo4jAl, String node_label, String name,String comId){
        String req = String.format("MATCH(a: `%s`{name: '%s'}) SET a.communityID='%s' RETURN a.communityID as comID" , node_label, name,comId);
        
        neo4jAl.info(req);

      
        try{

            Result result = neo4jAl.executeQuery(req); 

           // List<String>res = new ArrayList<>();

            if(!result.hasNext()){

                return new String();

            } 
			else {

               return ((String)result.next().get("comID")); 
               
            }

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		}
    }

    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param comId
     * @return
     */

    public static List<String> nodeName(Neo4jAl neo4jAl, String node_label,  String comId ){

        String req =  String.format("MATCH (a:`%s`{communityID:'%s'})UNWIND a.name AS name RETURN collect(distinct name) as name" , node_label,comId);

        try {
			Result result = neo4jAl.executeQuery(req);
			neo4jAl.info(String.format("result : [%s] ", result));
			if(!result.hasNext()) return new ArrayList<>();
			else return ((List<String>) result.next().get("name")).stream()
					.sorted().collect(Collectors.toList());
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the nodes " ), e);
			throw new Error("Failed to get label's keys");
		}
    }

    /**
     * 
     * @param neo4jAl
     * @param name
     * @return
     * @throws Neo4jQueryException
     */

    public static Optional<Node>createNode(Neo4jAl neo4jAl, String name) throws Neo4jQueryException{
		String req = String.format("MERGE (o: `Community` {name: '%s'})  RETURN o as node", name ); 

		neo4jAl.info(req);
		try{
			Result res = neo4jAl.executeQuery(req); 

			return  Optional.ofNullable((Node) res.next().get("node"));

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the node. Request : %s", req), e);
			throw new Neo4jQueryException("Failed to get the node.", e, "NEO4JUTILS");
		}
		
	}

    /**
     * 
     * @param neo4jAl
     * @param start
     * @param end
     * @return
     * @throws Neo4jQueryException
     */

    public static Relationship createRels(Neo4jAl neo4jAl, String start, String end) throws Neo4jQueryException{
        	// Create the relationship
		String req = String.format("MATCH (a : `Community` {name : '%s'}), (b:`Community` {name : '%s'})" +
        "MERGE (a)-[r:`CONNECT`]->(b) " +
        "RETURN r as relationship ", start,end );

        neo4jAl.info(req); 

        try {

            Result res; 

            Relationship rel = null; 

          //  neo4jAl.info(String.format("weight: %s", weight.toString()));


            res = neo4jAl.executeQuery(req);

            if(!res.hasNext()){

                throw new Error("Failed to create the relationship. No return resulted"); 
            }


            else{

                rel = (Relationship) res.next().get("relationship"); 

                neo4jAl.info(rel.toString()); 

            }
            

            return rel; 

        } catch (Exception | Neo4jQueryException e) {
            neo4jAl.error("Failed to get the relationship.", e);
            throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
        }
    }

    public static Relationship newRels(Neo4jAl neo4jAl, String node_label, String start, String end) throws Neo4jQueryException{
        // Create the relationship
    String req = String.format("MATCH (a : `%s` {name : '%s'}), (b:`%s` {name : '%s'})" +
    "MERGE (a)-[r: `CONNECT`]->(b) " +
    "RETURN r as relationship ", node_label,start,node_label,end );

    neo4jAl.info(req); 

    try {

        Result res; 

        Relationship rel = null; 

      //  neo4jAl.info(String.format("weight: %s", weight.toString()));


        res = neo4jAl.executeQuery(req);

        if(!res.hasNext()){

            throw new Error("Failed to create the relationship. No return resulted"); 
        }


        else{

            rel = (Relationship) res.next().get("relationship"); 

            neo4jAl.info(rel.toString()); 

        }
        

        return rel; 

    } catch (Exception | Neo4jQueryException e) {
        neo4jAl.error("Failed to get the relationship.", e);
        throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
    }
}


    public static Optional<Relationship> findRelationship(Neo4jAl neo4jAl, String node_label, String rels_label, String start, String end)
			throws Neo4jQueryException {
		String req = String.format("MATCH (a : `%s` {`name` : '%s'})-[r:`%s`]->(b:`%s` {`name` : '%s'}) "  +
				"RETURN r as relationship", node_label, start, rels_label,node_label,end);
	
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


    
	 public static List<String> getName(Neo4jAl neo4jAl, String node_label){

		String request = String.format("MATCH (a:`%s`) UNWIND a.`name` AS types RETURN collect(distinct types) as type", node_label);
		neo4jAl.info(request); 
		try{

			Result result = neo4jAl.executeQuery(request);
			neo4jAl.info(String.format("result: [%s]", result));
			if(!result.hasNext()) return new ArrayList<>();
			else return ((List<String>) result.next().get("type")).stream().sorted().collect(Collectors.toList()); 
			
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of types"), e);
			throw new Error("Failed to get label's types");
		}

		}
    





    /* 

    public List<String> deleteGraph(Neo4jAl neo4jAl, String node_label, String rels_label){

        String graphName = graphName(neo4jAl, node_label, rels_label); 

        String req = String.format("CALL gds.graph.drop('%s')", graphName); 

        try {
			Result result = neo4jAl.executeQuery(req);
			neo4jAl.info(String.format("result : [%s] ", result));
			if(!result.hasNext()) return new ArrayList<>();
			else return (List<String>) result.next(); 
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the nodes " ), e);
			throw new Error("Failed to get label's keys");
		}


    }

    */ 


    


    /* 


    public static String groupNodes(Neo4jAl neo4jAl, String node_label){

        String request = String.format("CALL apoc.nodes.group(['%s'],['communityID'],[{`*`:'count', name:'collect'}])YIELD nodes, relationships  UNWIND nodes as node UNWIND relationships as rels RETURN node, rels", node_label);
        	
		neo4jAl.info(request);
		
		try {
			Result result = neo4jAl.executeQuery(request); 

			if(!result.hasNext()){
				
				return new String();

			}		
			
			else return ((String) result.next().get("rels"));


		} catch (ClassCastException e){
			neo4jAl.error(String.format("Wrong Cast Type. Please use Number Types"),e);
			throw new Error("Fail to get the weight of relationship");
		
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the weight list of relationships for label"), e);
			throw new Error("Failed to get the weight");

		}



    }

    */ 

    
    /* 

    public static List<String>labelProp(Neo4jAl neo4jAl, String node_label, String rels_label){

        String graphName = graphName(neo4jAl, node_label, rels_label); 
        String req = String.format("CALL gds.louvain.stream('%s') YIELD nodeId, communityId AS community RETURN gds.util.asNode(nodeId).name AS name, community", graphName); 





    }

    

    public static List<String>weaklyconnected(Neo4jAl neo4jAl, String node_label, String rels_label){

        String graphName = graphName(neo4jAl, node_label, rels_label); 
        String req = String.format("CALL gds.louvain.stream('%s') YIELD nodeId, componentId  RETURN gds.util.asNode(nodeId).name AS name, componenetId", graphName); 



    }

    */ 

    


    //louvain algorithm 

    //label propagation 

    //weakly connected components 

   
}
