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
import java.util.Random;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.samples.kmeans.KmeansDataGen;
import cgl.imr.types.StringValue;

/**
 * Generate data points to run K-means clustering.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */
public class KmeansDataGenMapTask implements MapTask {

	public static int MAX_VALUE = 500;
	public static int VECTOR_SIZE = 2;

	private JobConf jobConf;

	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		this.jobConf = jobConf;
	}

	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
	
		long numDataPoints = Long.parseLong(jobConf.getProperty(KmeansDataGen.PROP_NUM_DATA_PER_MAP));
		int numCentroids = Integer.parseInt(jobConf.getProperty(KmeansDataGen.NUM_CENTROIDS));
		int vectLength = Integer.parseInt(jobConf.getProperty(KmeansDataGen.VECT_LENGTH));
		StringValue fileName = (StringValue) val;
		String file;

		try {
			file = TwisterConfigurations.getInstance().getLocalDataDir() + "/"
					+ fileName.toString();

			Random rand = new Random(System.nanoTime());
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			// First line is the number of data points.
			String numPoints = numDataPoints + "\n";
			writer.write(numPoints);
			// Second line is the size of the vectors.
			String vecLen = vectLength + "\n";
			writer.write(vecLen);
			String strLine;
			for (int i = 0; i < numDataPoints; i++) {
				strLine = "";
				for (int j=0; j<vectLength; j++){
					strLine += rand.nextInt(MAX_VALUE) + " ";
				}
				strLine += "\n";
				writer.write(strLine);
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}
}
