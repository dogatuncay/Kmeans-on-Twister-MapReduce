package cgl.imr.samples.kmeans;

import java.util.Iterator;
import java.util.Map;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.types.DoubleValue;

public class KMeansObjValCombiner implements Combiner {
	
	DoubleValue result;
	
	public KMeansObjValCombiner() {
		result = new DoubleValue();
	}
	
	public void close() throws TwisterException {
		// TODO: Auto-generated method stub
	} 

	public void combine(Map<Key, Value> keyValues) throws TwisterException {
		assert (keyValues.size() == 1);
		Iterator<Key> ite = keyValues.keySet().iterator();
		Key key = ite.next();
		DoubleValue val = (DoubleValue) keyValues.get(key);
		try {
			this.result.fromBytes(val.getBytes());
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}
	
	public void configure(JobConf jobConf) throws TwisterException {
		// TODO: Auto-generated method stub
	}
	
	public DoubleValue getResult() {
		return result;
	}
}
