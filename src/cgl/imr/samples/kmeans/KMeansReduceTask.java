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

import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.BytesValue;
import cgl.imr.types.DoubleVectorData;

/**
 * Calculates the new centroids using the partial centroids.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */
public class KMeansReduceTask implements ReduceTask {

	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	public void configure(JobConf jobConf, ReducerConf reducerConf)
			throws TwisterException {
	}

	public void reduce(ReduceOutputCollector collector, Key key, 
			List<Value> values) throws TwisterException {

		if (values.size() <= 0) {
			throw new TwisterException("Reduce input error no values.");
		}
		try {
			BytesValue val = (BytesValue) values.get(0);
			DoubleVectorData tmpCentroids = new DoubleVectorData();
			tmpCentroids.fromBytes(val.getBytes());

			int numData = tmpCentroids.getNumData();
			/**
			 * One additional location carries the count.
			 */
			int lenData = tmpCentroids.getVecLen() - 1;

			double[][] newCentroids = new double[numData][lenData];

			DoubleVectorData centroids = null;
			double[][] tmpCentroid;
			double[] counts = new double[numData];
			int numMapTasks = values.size();
			for (int i = 0; i < numMapTasks; i++) {
				val = (BytesValue) values.get(i);
				centroids = new DoubleVectorData();
				centroids.fromBytes(val.getBytes());
				tmpCentroid = centroids.getData();

				for (int j = 0; j < numData; j++) {
					for (int k = 0; k < lenData; k++) {
						newCentroids[j][k] += tmpCentroid[j][k];
					}
					/*
					 * Say data length is 2, then the tmpCentroid has 3 data
					 * points including the counts. from points 0,1,2 , 2 is the
					 * count.
					 */
					counts[j] += tmpCentroid[j][lenData];
				}
			}

			/**
			 * The results have already been divided by the total number of data
			 * points. So simply adding them is enough.
			 */

			for (int i = 0; i < numData; i++) {
				for (int j = 0; j < lenData; j++) {
					if (counts[i] != 0) {
						newCentroids[i][j] = (newCentroids[i][j]) / counts[i];
					}
				}
			}

			DoubleVectorData newCentroidData = 
				new DoubleVectorData(newCentroids, numData, lenData);
			collector.collect(key, new BytesValue(newCentroidData.getBytes()));
			
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}
}
