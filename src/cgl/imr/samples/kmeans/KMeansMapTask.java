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

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.BytesValue;
import cgl.imr.types.DoubleVectorData;
import cgl.imr.types.StringKey;

/**
 * Map task for the K-Means clustering.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */
public class KMeansMapTask implements MapTask {

	private FileData fileData;
	private DoubleVectorData vectorData;

	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	/**
	 * Loads the vector data from a file. Since the map tasks are cached
	 * across iterations, we only need to load this data  once for all
	 * the iterations.
	 */
	public void configure(JobConf jobConf, MapperConf mapConf)	throws TwisterException {
		this.vectorData = new DoubleVectorData();
		fileData = (FileData) mapConf.getDataPartition();
		try {
			vectorData.loadDataFromTextFile(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	public double getEuclidean2(double[] v1, double[] v2, int vecLen) {
		double sum = 0;
		for (int i = 0; i < vecLen; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		return sum; // No need to use the sqrt.
	}

	/**
	 * Map function for the K-means clustering. Calculates the Euclidean
	 * distance between the data points and the given cluster centers. Next it
	 * calculates the partial cluster centers as well.
	 */
	
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {

		double[][] data = vectorData.getData();
		DoubleVectorData cData = new DoubleVectorData();

		try {
			cData.fromBytes(val.getBytes());
			double[][] centroids = cData.getData();

			int numCentroids = cData.getNumData();
			int numData = vectorData.getNumData();
			int vecLen = vectorData.getVecLen();
			double newCentroids[][] = new double[numCentroids][vecLen + 1];

			for (int i = 0; i < numData; i++) {
				double min = 0;
				double dis = 0;
				int minCentroid = 0;
				for (int j = 0; j < numCentroids; j++) {
					dis = getEuclidean2(data[i], centroids[j], vecLen);
					if (j == 0) {
						min = dis;
					}
					if (dis < min) {
						min = dis;
						minCentroid = j;
					}
				}

				for (int k = 0; k < vecLen; k++) {
					newCentroids[minCentroid][k] += data[i][k];
				}
				newCentroids[minCentroid][vecLen] += 1;
			}

			/**
			 * additional location carries the number of partial points to a
			 * particular centroid.
			 */
			DoubleVectorData newCData = new DoubleVectorData(newCentroids,
					numCentroids, vecLen + 1);
			// This algorithm uses only one reduce task, so we only need one
			// key.
			collector.collect(new StringKey("kmeans-map-to-reduce-key"),
					new BytesValue(newCData.getBytes()));

		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}
}
