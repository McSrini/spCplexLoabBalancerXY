/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.mcmaster.sploadbalancexy.heuristics;

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
 * Tries to make sure the number of leaf nodes on every partition is above a threshold
 * 
 * The threshold is the minimum of a parameter  , and the average count across partitions
 * 
 * Uses raw leaf nodes for balancing
 * 
 */
public class AveragingHeuristic {
        
   
   private static Logger logger=Logger.getLogger(AveragingHeuristic.class);
   
   //this is the threshold the load balancer tries to achieve on each partition
   private int threshold ;
         
   //how many nodes are available on each partition ? These need to be averaged out across partitions.
   private Map <Integer, Integer > availableNodesMap =new HashMap<Integer, Integer >();  
    
   private List<ActiveSubtreeCollection> partitionList;
   
   //make note of how many total nodes each partition has, at the outset
   private Map <Integer, Integer > currentTotalNodeCountMap = new HashMap<Integer, Integer >();
   
   //from each partition, how many to move?
   //-ve number indicates movement away
   private Map <Integer, Integer > howManyNodesToMoveMap = new HashMap<Integer, Integer >();
   
   //do not do this on spark
   private List <NodeAttachment> consolidatedFarmedNodeList= new ArrayList <NodeAttachment>();
   
   //count of nodes to pluck out from each partition
   public Map <Integer, List <NodeAttachment>> nodesToPluckOutMap = new HashMap <Integer,  List <NodeAttachment>>();
   
   //this  map dictates where the plucked out nodes are going
   public Map <Integer, Integer> nodesToMoveIn  = new HashMap <Integer, Integer>();
   
   static {
       
       logger.setLevel(Level.DEBUG);
       PatternLayout layout = new PatternLayout("%5p  %d  %F  %L  %m%n");     
       try {
           logger.addAppender(new RollingFileAppender(layout,LOG_FOLDER+AveragingHeuristic.class.getSimpleName()+""+ LOG_FILE_EXTENSION));
           logger.debug("AveragingHeuristic Version 1.0");
       } catch (IOException ex) {
           //java.util.logging.Logger.getLogger(AveragingHeuristic.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
       }
         
   }   
   
   public AveragingHeuristic (List<ActiveSubtreeCollection> partitionList) {
       
       this.partitionList=partitionList;
       
       for (int index = ZERO; index <partitionList.size(); index++){
           int rawnodeCount = partitionList.get(index).getRawNodesCount();
           int treenodeCount =  partitionList.get(index).getNumberOFLeafsAcrossAllTrees();
           availableNodesMap.put(index, rawnodeCount);
           this.currentTotalNodeCountMap .put(index, treenodeCount +rawnodeCount);
       }
       
       //find the minimum number of leafs each partition must have, after load balancing
       //It is the minimum of a pre-defined parameter, and the current average
       //
       
       threshold  = Math.min(LEAF_THRESHOLD_PER_PARTITION_AFTER_BALANCING ,  average ());
         
      
   }
   
   public boolean isLoadBalancingRequired () {
       boolean result = false;
       
       //check if any partition has less than the user specified minimum number of leaf nodes
       for (Map.Entry <Integer, Integer > entry : currentTotalNodeCountMap.entrySet()){
           if (entry.getValue() <  -ONE+ threshold) {
               result = true ;
               break;
           }
       }
       
       return result;
   }
   
   public void loadBalance () throws Exception {
        
       logger.info("load balancing started ... ");
       //initialize maps which record how many nodes to move
       for (Integer partitionID : this.availableNodesMap.keySet()){          
          howManyNodesToMoveMap.put(partitionID, ZERO);
               
       }
       
       //move one node from richest to poorest
       while (ZERO < this.moveOneNode( )) {  }
       
      
      
       //at this point, we know how many nodes to move from where to where
       
       
       // find  nodes to pluck out  
       populateNodesToPluckOut( );
        prepareConsolidatedFarmedOutList();
              
       //now mark the partitions that are the recievers of plucked out nodes, and how many each will recieve
       findNodesToMoveIn(); 
       redistributeNodes();
       logger.info("load balancing completed");
   }
   /*
   private void printNodesToMoveIn () {
       logger.debug("Printing nodes to move in map");
       for (Entry <Integer, Integer> entry : this.nodesToMoveIn.entrySet()){
           logger.debug(entry.getKey() + " , " + entry.getValue());
       }
   }
   */
   
   private void populateNodesToPluckOut() {
         
       for (Map.Entry <Integer, Integer > entry :  howManyNodesToMoveMap.entrySet()){
           int partitionId = entry.getKey();
           int count = entry.getValue();
           if (count < ZERO) {
               
               //pluck  abs(count)  nodes
               nodesToPluckOutMap.put(partitionId, partitionList.get(partitionId).pluckRawNodes( -count)) ;
               
           }
       }
       
   }
   
   //this function populates nodesToMoveIn, basically deciding how many nodes to recieve into which partition
   private void findNodesToMoveIn() {
       for (Map.Entry <Integer, Integer > entry :  howManyNodesToMoveMap.entrySet()){
           int partitionId = entry.getKey();
           int count = entry.getValue();
            
           if (count > ZERO) {
               
               //this partition is a reciever
               nodesToMoveIn.put(partitionId, count);
           }
       }
   }
   
   private void prepareConsolidatedFarmedOutList(){
       
       for(List <NodeAttachment> list : this.nodesToPluckOutMap.values()){
           consolidatedFarmedNodeList.addAll( list);
       }
       
       
   }
   
   private   void redistributeNodes( ) throws Exception {
        for (Map.Entry <Integer, Integer> entry : nodesToMoveIn.entrySet()){
            
            int partitionID = entry.getKey();
            ActiveSubtreeCollection astc = partitionList.get(partitionID);
            
            int count = entry.getValue();
            
            while (count>ZERO&&consolidatedFarmedNodeList.size()>ZERO) {                
                astc.add(this.consolidatedFarmedNodeList.remove(-ONE+consolidatedFarmedNodeList.size()));
                count --;
            }
        }
    }
    
   
   private int moveOneNode( ) {
       int countMoved = ZERO;
       
       //as long as someone is below the threshold, get them one node from someone highest above the threshold
       int poorestPartitionID = findPoorestPartition();
       int richestPartitionID = this.findRichestPartition(   );
       if (richestPartitionID>=ZERO &&
               currentTotalNodeCountMap.get(poorestPartitionID) < this.threshold  &&
           currentTotalNodeCountMap.get(richestPartitionID) > this.threshold    ) {
           
                //move 1 node
               
                currentTotalNodeCountMap.put(poorestPartitionID,
                        currentTotalNodeCountMap.get(poorestPartitionID) +ONE);
                   
                currentTotalNodeCountMap.put(richestPartitionID,
                        currentTotalNodeCountMap.get(richestPartitionID) - ONE );
                howManyNodesToMoveMap.put( poorestPartitionID, 
                        howManyNodesToMoveMap .get(poorestPartitionID) +ONE);
                howManyNodesToMoveMap.put( richestPartitionID, 
                        howManyNodesToMoveMap.get(richestPartitionID) -ONE  );
               
                this.availableNodesMap.put(richestPartitionID,
                        -ONE+availableNodesMap.get(richestPartitionID ));
               
               
               countMoved = ONE;
       }
       
       return countMoved;
   }
      
   private int findPoorestPartition(){
       int partitionID = -ONE;
       long highCount = PLUS_INFINITY;
       for (Map.Entry <Integer, Integer > entry : this.currentTotalNodeCountMap.entrySet()){
           if (entry.getValue()<highCount){
               partitionID = entry.getKey();
               highCount = entry.getValue();
           }
       }
       return partitionID;
   }
   
   private int findRichestPartition( ){
       int partitionID = -ONE;
       long highCount = -ONE;
       for (Map.Entry <Integer, Integer > entry : currentTotalNodeCountMap.entrySet() ){
           if (entry.getValue()>highCount && null !=this.availableNodesMap.get(entry.getKey()) 
                   && ZERO<this.availableNodesMap.get(entry.getKey())){
               
               partitionID = entry.getKey();
               highCount = entry.getValue();
               
           }
       }
       return partitionID;
   }
    
   //find node count per partition
   private int  average () {
        double count = ZERO ;
        for (Map.Entry <Integer, Integer> entry : this.currentTotalNodeCountMap.entrySet()){
           count +=entry.getValue() ;
           
        }
        
        return  (int) Math.ceil(count/NUM_PARTITIONS);
   }
    
}
