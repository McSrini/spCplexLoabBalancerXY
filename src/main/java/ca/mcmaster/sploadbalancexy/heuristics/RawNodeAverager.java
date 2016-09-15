/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.mcmaster.sploadbalancexy.heuristics;

import org.apache.log4j.Logger;

import static ca.mcmaster.spcplexlibxy.Constants.*;
import static ca.mcmaster.spcplexlibxy.Parameters.*;
import ca.mcmaster.spcplexlibxy.datatypes.*;
import static ca.mcmaster.sploadbalancexy.Parameters.LEAF_THRESHOLD_PER_PARTITION_AFTER_BALANCING;
import java.io.IOException;
import java.util.*; 
import java.util.Map.Entry;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 *
 * @author srini
 * 
 * extracts all raw nodes from every partition, and randomly distributed them back evenly
 */
public class RawNodeAverager {
    
    private static Logger logger=Logger.getLogger(RawNodeAverager.class);
    
    //this is the threshold the load balancer tries to achieve on each partition
    private int threshold ;
    
    private List<ActiveSubtreeCollection> partitionList;
    
    //how many nodes are available on each partition ? These need to be averaged out across partitions.
    private Map <Integer, Integer > availableNodesMap =new HashMap<Integer, Integer >();  
   
    static {
       
        logger.setLevel(Level.DEBUG);
        PatternLayout layout = new PatternLayout("%5p  %d  %F  %L  %m%n");     
        try {
           logger.addAppender(new RollingFileAppender(layout,LOG_FOLDER+RawNodeAverager.class.getSimpleName()+""+ LOG_FILE_EXTENSION));
           logger.debug("RawNodeAverager Version 1.0");
        } catch (IOException ex) {
           //java.util.logging.Logger.getLogger(AveragingHeuristic.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
         
    }  
    
    public RawNodeAverager (List<ActiveSubtreeCollection> partitionList) {
       
       this.partitionList=partitionList;
       
       for (int index = ZERO; index <partitionList.size(); index++){
           int rawnodeCount = partitionList.get(index).getRawNodesCount();
           availableNodesMap.put(index, rawnodeCount);
           
       }
       
       //find the minimum number of leafs each partition must have, after load balancing
       //It is the minimum of a pre-defined parameter, and the current average
       //
       
       threshold  = Math.min(LEAF_THRESHOLD_PER_PARTITION_AFTER_BALANCING ,  average ());
         
      
    }
    
    public void loadBalance () throws Exception {

        logger.info("load balancing started ... ");
        
        //pluck out all raw nodes from every partition, consolidate and randomize them
        List <NodeAttachment> consolidatedFarmedNodeList= new ArrayList <NodeAttachment>();
        for (int index = ZERO; index <partitionList.size(); index++){
           int rawnodeCount = partitionList.get(index).getRawNodesCount();
           consolidatedFarmedNodeList.addAll(partitionList.get(index).pluckRawNodes(rawnodeCount ));   
        }
        
        //distribute threshold-1 nodes to each partition
        Collections.shuffle(consolidatedFarmedNodeList);
        for (int index = ZERO; index <partitionList.size(); index++){
            ActiveSubtreeCollection astc = partitionList.get(index);
            astc.add( removeNodes( consolidatedFarmedNodeList, -ONE+this.threshold) );
        }
        
        //distribute reamining nodes 1 by 1 to each partition
        for (int index = ZERO; index <partitionList.size() &&consolidatedFarmedNodeList.size()>ZERO ; index++){
            ActiveSubtreeCollection astc = partitionList.get(index);
            astc.add( removeNodes( consolidatedFarmedNodeList,  ONE) );
        }
        
        if (consolidatedFarmedNodeList.size()>ZERO)  logger.error( "Some nodes were not redistributed by the loadbabalncer");
        
        logger.info("load balancing completed ");
    }
    
    public boolean isLoadBalancingRequired () {
       //boolean result = false;
       
       /*
       //check if any partition has less than the user specified minimum number of leaf nodes
       for (Map.Entry <Integer, Integer > entry : availableNodesMap.entrySet()){
           if (entry.getValue() <  -ONE+ threshold) {
               result = true ;
               break;
           }
       }
       */
       List<Integer> listOfCounts = new ArrayList (availableNodesMap.values());
       Collections.sort(listOfCounts);
       
       int richestCount = listOfCounts.get(ZERO);
       
       Collections.reverse(listOfCounts);
       
       int poorestCount = listOfCounts.get(ZERO);
       
       //return result;
       return Math.abs(poorestCount-richestCount) >= TWO;
    }
      
    //find node count per partition
    private int  average () {
        double count = ZERO ;
        for (Map.Entry <Integer, Integer> entry : this.availableNodesMap.entrySet()){
           count +=entry.getValue() ;
           
        }
        
        return  (int) Math.ceil(count/NUM_PARTITIONS);
    }
    
    private  List <NodeAttachment> removeNodes( List <NodeAttachment> nodeList , int count){
         List <NodeAttachment> result= new ArrayList <NodeAttachment>();
         for (int index = ZERO; index < count ; index ++) {
             result.add(nodeList.remove( nodeList.size()-ONE)) ;
         }
         return result;
    }
    
}
