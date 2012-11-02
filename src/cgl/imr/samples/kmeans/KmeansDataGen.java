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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.types.IntKey;
import cgl.imr.types.StringValue;

/**
 * Generate data for K-Means clustering using MapReduce. It uses a "map-only"
 * operation to generate data concurrently.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */
public class KmeansDataGen {

	public static String DATA_FILE_SUFFIX = ".txt";
	//public static int NUM_CENTROIDS = 64;
	public static String PROP_NUM_DATA_PER_MAP = "data_points_per_map";
	public static String NUM_CENTROIDS = "num_centroids";
	public static String VECT_LENGTH = "vect_length";

	public static void generateInitClusterCenters(String initCenterFile, int numCentroids, int vectLength) {
		Random rand = new Random(System.nanoTime());
		try {
			System.out.println("InitCentroid:");
			BufferedWriter writer = new BufferedWriter(new FileWriter(initCenterFile));

			String line = numCentroids + "\n";
			writer.write(line);
			System.out.println(line);
			line = vectLength + "\n";
			writer.write(line);
			System.out.println(line);

			for (int i = 0; i < numCentroids; i++) {
				line = "";
				for (int j = 0; j<vectLength; j++){
					line += rand.nextInt(KmeansDataGenMapTask.MAX_VALUE)+" ";
				}
				line += "\n";
				writer.write(line);
				System.out.println(line);
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Produces a list of key,value pairs for map tasks.
	 * 
	 * @param numMaps
	 *            - Number of map tasks.
	 * @return - List of key,value pairs.
	 */
	private static List<KeyValuePair> getKeyValuesForMap(int numMaps,
			String dataFilePrefix, String dataDir) {
		List<KeyValuePair> keyValues = new ArrayList<KeyValuePair>();
		IntKey key = null;
		StringValue value = null;
		for (int i = 0; i < numMaps; i++) {
			key = new IntKey(i);
			value = new StringValue(dataDir + "/" + dataFilePrefix + i
					+ DATA_FILE_SUFFIX);
			keyValues.add(new KeyValuePair(key, value));
		}
		return keyValues;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("kmeans args.len:"+args.length);
		if (args.length != 7) {
			String errorReport = "KmeansDataGen: The Correct arguments are \n"
					+ "java cgl.mr.kmeans.KmeansDataGen  [init clusters file][num centroids][vector length][sub dir][data file prefix][num splits=num maps][num data points]";
			System.out.println(errorReport);
			System.exit(0);
		}

		/**
		 * Total data points should be = n x numMaps x 64, where n can be any
		 * integer.
		 */

		String initClustersFile = args[0];
		int numCentroids = Integer.parseInt(args[1]);
		int vecLength = Integer.parseInt(args[2]);
		String dataDir = args[3];
		String dataFilePrefix = args[4];
		int numMapTasks = Integer.parseInt(args[5]);
		long numDataPoints = Long.parseLong(args[6]);
		long numDataPointsPerMap = numDataPoints / numMapTasks;
		
		// Generate initial cluster centers sequentially.
		generateInitClusterCenters(initClustersFile, numCentroids, vecLength);

		
		if (numDataPoints % numMapTasks != 0) {
			System.out.print("Number of data points are not equally divisable to map tasks ");
			System.exit(0);
		}

		KmeansDataGen client;
		try {
			client = new KmeansDataGen();
			//client.driveMapReduce(numMapTasks, numDataPointsPerMap,dataFilePrefix, dataDir);
			client.driveMapReduce(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	//public void driveMapReduce(int numMapTasks, long dataPointsPerMap,
	//String dataFilePrefix, String dataDir) throws Exception {
	
	public void driveMapReduce(String[] args) throws Exception {
		int numCentroids = Integer.parseInt(args[1]);
		int vecLength = Integer.parseInt(args[2]);
		String dataDir = args[3];
		String dataFilePrefix = args[4];
		int numMapTasks = Integer.parseInt(args[5]);
		long numDataPoints = Long.parseLong(args[6]);
		long numDataPointsPerMap = numDataPoints / numMapTasks;
		
		int numReducers = 0; // We don't need any reducers.

		// JobConfigurations
		JobConf jobConf = new JobConf("kmeans-data-gen"	+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(KmeansDataGenMapTask.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		jobConf.addProperty(PROP_NUM_DATA_PER_MAP, String.valueOf(numDataPointsPerMap));
		jobConf.addProperty(NUM_CENTROIDS, args[1]);
		jobConf.addProperty(VECT_LENGTH, args[2]);
		

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps();
		TwisterMonitor monitor = driver.runMapReduce(getKeyValuesForMap(
				numMapTasks, dataFilePrefix, dataDir));
		monitor.monitorTillCompletion();
		driver.close();
	}

}
