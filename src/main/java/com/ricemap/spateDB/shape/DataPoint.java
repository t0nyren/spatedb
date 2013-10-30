package com.ricemap.spateDB.shape;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.ricemap.spateDB.io.TextSerializerHelper;

public class DataPoint extends Point3d {
	private static final byte[] Separator = {','};
	
	public long id;
	public int dataValue;
	
	public DataPoint() {
		// TODO Auto-generated constructor stub
	}

	public DataPoint(double t, double x, double y, long id, int dataValue) {
		
		super(t, x, y);
		this.id = id;
		this.dataValue = dataValue;
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void write (DataOutput out) throws IOException{
		super.write(out);
		out.writeLong(id);
		out.writeInt(dataValue);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException{
		super.readFields(in);
		this.id = in.readLong();
		this.dataValue = in.readInt();
	}
	
	@Override
	public Text toText(Text text){
		super.toText(text);
		text.append(Separator, 0, Separator.length);
		TextSerializerHelper.serializeLong(id, text, ',');
		TextSerializerHelper.serializeInt(dataValue, text, '\0');
		return text;
	}
	
	@Override
	public void fromText(Text text){
		byte[] bytes = text.getBytes();
		super.fromText(text);
		text.set(bytes, 1, text.getLength() - 1);
		id = TextSerializerHelper.consumeLong(text, ',');
		dataValue = TextSerializerHelper.consumeInt(text, '\n');
	}

}
