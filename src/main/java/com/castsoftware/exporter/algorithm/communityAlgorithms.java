
package com.castsoftware.exporter.algorithm;
import com.castsoftware.exporter.csv.Formatter;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.database.Neo4jAlUtils;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;


import java.lang.reflect.Array;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.HashMap;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Relationship;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.print.attribute.standard.MediaSize.Other;

import java.util.Optional;

public class communityAlgorithms {

    private Neo4jAl neo4jAl;

    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void louvainAlgo(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{

       // List<String> delete = algorithmsUtils.deleteGraph(neo4jAl, node_label,rels_label); 

        Map<String,String> louvain = algorithmsUtils.louvainAlgo(neo4jAl, node_label, rels_label);

        processOutput(neo4jAl,louvain, node_label,rels_label);

     }
    
    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void labelProp(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{

        Map<String,String> labelProp= algorithmsUtils.labelPropAlgo(neo4jAl, node_label, rels_label);

        processOutput(neo4jAl,labelProp, node_label, rels_label);

    }

    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void weaklyAlgo(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{

        Map<String,String> weaklyAlgo= algorithmsUtils.weaklyConnected(neo4jAl, node_label, rels_label);

        processOutput(neo4jAl,weaklyAlgo, node_label,rels_label);

    }

    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void weightLabelProp(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{

        Map<String, String> weightLabelProp = algorithmsUtils.weightLabelProp(neo4jAl, node_label, rels_label); 
        processOutput(neo4jAl, weightLabelProp, node_label,rels_label);

    }

    /**
     * 
     * @param neo4jAl
     * @param algoProcess
     * @param node_label
     * @throws Neo4jQueryException
     */
    private static void processOutput(Neo4jAl neo4jAl, Map<String,String>algoProcess, String node_label, String rels_label) throws Neo4jQueryException{

        List<String> name = new ArrayList<>(); 
        List<String> comId = new ArrayList<>(); 

        for(String key: algoProcess.keySet()){
         
           name.add(key);

           neo4jAl.info (name.toString()); 
        }

        for(String values:algoProcess.values()){
            
            comId.add(values); 

            neo4jAl.info(comId.toString());  
        }

        neo4jAl.info(comId.toString()); 

        neo4jAl.info(String.format("hello: ", comId));

        List<String>id = new ArrayList<>(); 
        List<String> getName; 
        List<String> getValues; 
        String newPropValue = new String();


        for (int i = 0; i<name.size(); i++){

            getName = new ArrayList<>(); 
            getName.add(name.get(i));
            neo4jAl.info(getName.toString()); 
            String nameOut = String.join(",",getName); 
            neo4jAl.info(nameOut);

            getValues = new ArrayList<>(); 
            getValues.add(comId.get(i)); 
            neo4jAl.info(getValues.toString());
            String idOut = String.join(",",getValues); 
            neo4jAl.info(idOut); 

            //set the properties of the nodes 
            newPropValue = algorithmsUtils.setProp(neo4jAl, node_label, nameOut, idOut);
            
            //returning a list of comId without repeated values 
            if(!id.contains(idOut)){
                id.add(idOut);
            }

            neo4jAl.info(id.toString()); 

        }

        List<String>node = new ArrayList<>(); 

        Map <String, List<String>> output  = new HashMap<>(); 

        for(int i = 0; i<id.size(); i++){

            List<String>nameId = algorithmsUtils.nodeName(neo4jAl, node_label, id.get(i));
           // List<String>nameId2 = algorithmsUtils.nodeName(neo4jAl, node_label, id.get(j)); 
            neo4jAl.info(nameId.toString()); 
            output.put(id.get(i), nameId);
            neo4jAl.info(output.get(id.get(i)).toString());
            neo4jAl.info(id.get(i));
            neo4jAl.info("line"); 
            neo4jAl.output(output);

          // createNodeRels(neo4jAl,nameId, id.get(i));

           //compareName(neo4jAl, node_label, rels_label, id);

           createNodeRels(neo4jAl, nameId, id.get(i));



           // }

        }
        neo4jAl.output(output);
        
        compareName(neo4jAl, node_label, rels_label, id, output);


    }

    private static void createNodeRels(Neo4jAl neo4jAl,List<String>nameId, String idIndex) throws Neo4jQueryException{
        
        Optional<Node> nodeOptional = algorithmsUtils.createNode(neo4jAl, idIndex);           
        Node n = nodeOptional.get(); 

        List<String>nameValue = new ArrayList<>(); 

        for(int j = 0; j<nameId.size(); j++){
            
            nameValue = new ArrayList<>(); 
            nameValue.add(nameId.get(j)); 
            String sName = String.join(",",nameValue); 

            Optional<Node> createNode = algorithmsUtils.createNode(neo4jAl,sName);           
            Node n1= createNode.get(); 

            Relationship r = algorithmsUtils.createRels(neo4jAl, sName , idIndex); 

        }


    }

    private static void compareName(Neo4jAl neo4jAl, String node_label, String rels_label, List<String> id, Map<String, List<String>> output) throws Neo4jQueryException{

        // List<String> outputKey= new ArrayList<>(); 

        // for(String key: output.keySet()){
         
        //    outputKey.add(key);

        //    neo4jAl.info (outputKey.toString()); 
        // }

         List<String> firstComp = new ArrayList<>(); 
         List<String> secondComp = new ArrayList<>(); 

        // String firstComp = new String(); 
        // String secondComp = new String(); 
        

        for(int i = 0 ; i<id.size(); i++){

           // firstComp = new ArrayList<>(); 

            neo4jAl.info(id.get(i)); 
            firstComp = output.get(id.get(i));
          //  firstComp =output.get(id.get(i)).toString();

            neo4jAl.info(firstComp.toString()); 

            for (int j =i+1; j<id.size(); j++){

                //secondComp = new ArrayList<>(); 
                neo4jAl.info(id.get(j)); 
                secondComp = output.get(id.get(j));

                //secondComp = output.get(id.get(j)).toString();

                neo4jAl.info(secondComp.toString()); 

                neo4jAl.info("yuh"); 

                // List<String>list1 = Arrays.asList(firstComp);
                // List<String>list2 = Arrays.asList(secondComp);

                // neo4jAl.info(String.format("l1 : %s", list1));
                // neo4jAl.info(String.format("L2 : %s", list2));

               
                Relationship rels; 
                for (int k = 0 ; k<firstComp.size(); k++){

                    for(int l = 0 ; l<secondComp.size(); l++){

                        neo4jAl.info(firstComp.get(k)); 
                        neo4jAl.info(secondComp.get(l)); 


                        Optional<Relationship>findRels = algorithmsUtils.findRelationship(neo4jAl,node_label, rels_label, firstComp.get(k), secondComp.get(l)); 
                        
                        if(findRels.isPresent()){
                           

                            rels = algorithmsUtils.newRels(neo4jAl, node_label, id.get(i), id.get(j));
                            
                        }
                            
                        else{

                            rels = null; 
                        } 

                        Optional<Relationship>findRels2 = algorithmsUtils.findRelationship(neo4jAl,node_label, rels_label, firstComp.get(l), secondComp.get(k)); 
                        
                        if(findRels2.isPresent()){

                            rels = algorithmsUtils.newRels(neo4jAl, node_label, id.get(j), id.get(k));
                            
                        }
                            
                        else{

                            rels = null; 
                        } 
                           
                        

                    }

                }


            }

            



        }

        neo4jAl.info(firstComp.toString()); 
        neo4jAl.info(secondComp.toString()); 




        // List<String>getName = algorithmsUtils.getName(neo4jAl, node_label); 

        // Relationship r; 

        // for(int i = 0; i<getName.size(); i++ ){

        //     for (int j = 0; i<getName.size(); j++){

        //         Optional<Relationship>findRels = algorithmsUtils.findRelationship(neo4jAl,node_label, rels_label, getName.get(i), getName.get(j)); 

        //         if(findRels.isPresent()){

        //             for(int k = 0; i<id.size(); i++){

        //                 for(int l = 0 ; i<id.size(); l++){

        //                     r = algorithmsUtils.newRels(neo4jAl, node_label, id.get(k), id.get(l)); 

        //                 }

        //             }
                   
        //         }
        //     }
        // }

    }
    
    /* 
    public static List<String>weaklyconnected(){

    }


    //louvain algorithm 

    //label propagation 

    //weakly connected components 

    */ 


    /**
	 * Exporter
	 * @param neo4jAl
	 */
	public communityAlgorithms(Neo4jAl neo4jAl) {
		this.neo4jAl = neo4jAl;
	}





    
}


