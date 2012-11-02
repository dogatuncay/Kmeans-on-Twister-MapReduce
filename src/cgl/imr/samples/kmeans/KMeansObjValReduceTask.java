package cgl.imr.samples.kmeans;

import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.DoubleValue;

public class KMeansObjValReduceTask implements ReduceTask {
	
	public void close() throws TwisterException {
		// TODO: auto-generated method stub
	}
	
	public void configure(JobConf jobConf, ReducerConf reducerConf) throws TwisterException {
		
	}
	
	public void reduce(ReduceOutputCollector collector, Key key, List<Value> values) throws TwisterException {
		
		if (values.size() <= 0) {
			throw new TwisterException("Reduce input error no value.");
		}
		
		
		double objValue = 0;
		int numMapTasks = values.size();
		for (int i = 0; i < numMapTasks; i++) {
			DoubleValue val = (DoubleValue)values.get(i);
			objValue += val.getVal();
		}
		collector.collect(key,  new DoubleValue(objValue));
	}

}
