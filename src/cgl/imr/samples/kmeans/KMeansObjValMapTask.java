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
import cgl.imr.types.DoubleValue;
import cgl.imr.types.DoubleVectorData;
import cgl.imr.types.StringKey;

/*
 * Map taks for calculating the object value j of 
 * 
 */

public class KMeansObjValMapTask implements MapTask {
	private FileData fileData;
	private DoubleVectorData vectorData;
	
	public void close() throws TwisterException {
		//TODO auto-generated method stub
	}
	
	/**
	 * Loads points data from partition file
	 * All points here are static data*/
	public void configure(JobConf jobConf, MapperConf mapConf) throws TwisterException {
		vectorData = new DoubleVectorData();
		fileData = (FileData) mapConf.getDataPartition();
		try {
			vectorData.loadDataFromTextFile(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}
	
	public double getDist(double[] v1, double[] v2, int vecLen) {
		double sum = 0;
		for (int i = 0; i < vecLen; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		return sum;
	}
	
	/** 
	 * Map function for calculating the K-means object value.  */
	public void map(MapOutputCollector collector, Key key, Value val) throws TwisterException {
		double[][] pointsData = vectorData.getData();
		DoubleVectorData cData = new DoubleVectorData();
		
		try {
			cData.fromBytes(val.getBytes());
			double[][] centroids = cData.getData();
			
			int numCentroids = cData.getNumData();
			int numData = vectorData.getNumData();
			int vecLen = vectorData.getVecLen();
			double objectValue = 0;
			
			for (int i = 0; i < numData; i++) {
				double min = 0;
				double dist = 0;
				
				for (int j = 0; j < numCentroids; j++) {
					dist = getDist(pointsData[i], centroids[j], vecLen);
					if (j == 0) {
						min = dist;
					}
					if (dist < min) {
						min = dist;
					}
				}
				
				objectValue += min;				
			}
			
			collector.collect(new StringKey("kmeans-object-value-key"), new DoubleValue(objectValue));
			
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}
}
