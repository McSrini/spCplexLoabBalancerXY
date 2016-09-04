/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.mcmaster.sploadbalancexy;

import static ca.mcmaster.spcplexlibxy.Constants.*;



/**
 *
 * @author srini
 */
public class Parameters {
       
   //change this parameter if needed. 
    //Load balancing will not happen if number of leafs on every partition exceeds this.
   public static int LEAF_THRESHOLD_PER_PARTITION_AFTER_BALANCING = FIVE*THOUSAND;
   
  
   
}
