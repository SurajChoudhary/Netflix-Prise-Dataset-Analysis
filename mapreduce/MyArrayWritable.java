package bigdata.hbase.quan.mapreduce;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class MyArrayWritable extends ArrayWritable {

	public MyArrayWritable(Class<? extends Writable> valueClass,
			Writable[] values) {
		super(valueClass, values);
	}

	public MyArrayWritable(Class<? extends Writable> valueClass) {
		super(valueClass);
	}

	@Override
	public IntWritable[] get() {
		return (IntWritable[]) super.get();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		for (IntWritable i : get()) {
			i.write(arg0);
		}
	}
}
