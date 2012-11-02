/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package cgl.imr.samples.kmeans;

import java.io.IOException;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.DoubleValue;
import cgl.imr.types.DoubleVectorData;

/**
 * Implements K-means clustering algorithm using MapReduce programming model.
 * <p>
 * <code>
 * K-means Clustering Algorithm for MapReduce
 * 	Do
 * 	Broadcast Cn 
 * 	[Perform in parallel] the map() operation
 * 	for each Vi
 * 		for each Cn,j
 * 	Dij <= Euclidian (Vi,Cn,j)
 * 	Assign point Vi to Cn,j with minimum Dij		
 * 	for each Cn,j
 * 		Cn,j <=Cn,j/K
 * 	
 * 	[Perform Sequentially] the reduce() operation
 * 	Collect all Cn
 * 	Calculate new cluster centers Cn+1
 * 	Diff<= Euclidian (Cn, Cn+1)
 * 	while (Diff <THRESHOLD)
	 * </code>
 * <p>
 * The MapReduce algorithm we used is shown below. (Assume that the input is
 * already partitioned and available in the compute nodes). In this algorithm,
 * Vi refers to the ith vector, Cn,j refers to the jth cluster center in nth
 * iteration, Dij refers to the Euclidian distance between ith vector and jth
 * cluster center, and K is the number of cluster centers.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 */
public class KmeansClustering {

	public static String DATA_FILE_SUFFIX = ".txt";
	public static int NUM_LOOPS = 16;
	public static String PROP_VEC_DATA_FILE = "prop_vec_data_file";
	public static int THRESHOLD = 1;

	/**
	 * Main program to run K-means clustering.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			String errorReport = "KMeansClustering: the Correct arguments are \n"
					+ "java cgl.imr.samples.kmeans.KmeansClustering "
					+ "<centroid file> <num map tasks> <partition file>";
			System.out.println(errorReport);
			System.exit(0);
		}
		String centroidFile = args[0];
		int numMapTasks = Integer.parseInt(args[1]);
		String partitionFile = args[2];

		int numToRun = 10;
		while (numToRun-- > 0) {
			KmeansClustering client;
			try {
				client = new KmeansClustering();
				double beginTime = System.currentTimeMillis();
				client.driveMapReduce(partitionFile, numMapTasks, centroidFile);			
				double endTime = System.currentTimeMillis();
				System.out
						.println("------------------------------------------------------");
				System.out.println("Kmeans clustering took "
						+ (endTime - beginTime) / 1000 + " seconds.");
				System.out
						.println("------------------------------------------------------");
				KmeansDataGen.generateInitClusterCenters(centroidFile, 2, 3);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.exit(0);
	}

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public void driveMapReduce(String partitionFile, int numMapTasks,
			String centroidFile) throws Exception {
		long beforeTime = System.currentTimeMillis();
		int numReducers = 1; // we need only one reducer for the above

		// JobConfigurations
		JobConf jobConf = new JobConf("kmeans-map-reduce"+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(KMeansMapTask.class);
		jobConf.setReducerClass(KMeansReduceTask.class);
		jobConf.setCombinerClass(KMeansCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		//jobConf.setFaultTolerance();

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);

		DoubleVectorData cData = new DoubleVectorData();
		try {
			cData.loadDataFromTextFile(centroidFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//double totalError = 0;
		int loopCount = 0;
		TwisterMonitor monitor = null;

		//@SuppressWarnings("unused")
		// Use this with the while loop.
		//for (loopCount = 0; loopCount < NUM_LOOPS; loopCount++) {
		
		//Main iteration for K-Means clustering
		//boolean complete = false;
		while (loopCount < 10) {		
			monitor = driver.runMapReduceBCast(cData);
			monitor.monitorTillCompletion();
			DoubleVectorData newCData = ((KMeansCombiner) driver.getCurrentCombiner()).getResults();
			//totalError = getError(cData, newCData);
			cData = newCData;
			/*
			if (totalError < THRESHOLD) {
				complete = true;
				break;
			}
			*/			
			loopCount++;
		}
		// Print the test statistics
		double timeInSeconds = ((double) (System.currentTimeMillis() - beforeTime)) / 1000;
		double[][] selectedCentroids = cData.getData();
		int numCentroids = cData.getNumData();
		int vecLen = cData.getVecLen();

		for (int i = 0; i < numCentroids; i++) {
			for (int j = 0; j < vecLen; j++) {
				System.out.print(selectedCentroids[i][j] + " , ");
			}
			System.out.println();
		}
		
		//drive another MapReduce task to calculate the object value j
		try {
			objectValueCalcMapReduce(partitionFile, numMapTasks, cData);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Total Time for kemeans : " + timeInSeconds);
		System.out.println("Total loop count : " + (loopCount));
		// Close the TwisterDriver. This will close the broker connections and
		driver.close();
	}
	
	private void objectValueCalcMapReduce(String partitionFile, int numMapTasks, DoubleVectorData centroids) throws Exception {
		long startTime = System.currentTimeMillis();
		int numReducers = 1;
		
		JobConf jobConf = new JobConf("kmeans-object-value" + uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(KMeansObjValMapTask.class);
		jobConf.setReducerClass(KMeansObjValReduceTask.class);
		jobConf.setCombinerClass(KMeansObjValCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		
		TwisterDriver objValDriver = new TwisterDriver(jobConf);
		objValDriver.configureMaps(partitionFile);
		
		TwisterMonitor monitor = null;
		monitor = objValDriver.runMapReduceBCast(centroids);
		monitor.monitorTillCompletion();
		DoubleValue finalObjVal = ((KMeansObjValCombiner) objValDriver.getCurrentCombiner()).getResult();
		
		System.out.println("the final objective value is: " + finalObjVal.getVal());
		
		double timeInSeconds = ((double) (System.currentTimeMillis() - startTime)) / 1000;
		System.out.println("Total time for object value calculation: " + timeInSeconds);
		
		objValDriver.close();
		
	}

	private double getError(DoubleVectorData cData, DoubleVectorData newCData) {
		double totalError = 0;
		int numCentroids = cData.getNumData();

		double[][] centroids = cData.getData();
		double[][] newCentroids = newCData.getData();

		for (int i = 0; i < numCentroids; i++) {
			totalError += getEuclidean(centroids[i], newCentroids[i], cData
					.getVecLen());
		}
		return totalError;
	}

	/**
	 * Calculates the square value of the Euclidean distance. Although K-means
	 * clustering typically uses Euclidean distance, the use of its square value
	 * does not change the algorithm or the final results. Calculation of square
	 * root is costly. square value
	 * 
	 * @param v1
	 *            - First vector.
	 * @param v2
	 *            - Second vector.
	 * @param vecLen
	 *            - Length of the vectors.
	 * @return - Square of the Euclidean distances.
	 */
	private double getEuclidean(double[] v1, double[] v2, int vecLen) {
		double sum = 0;
		for (int i = 0; i < vecLen; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		return sum;
	}
}
