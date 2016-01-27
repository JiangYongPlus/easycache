package com.hazelcast.easycache.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoDateSerializer extends Serializer<java.sql.Date>{

	@Override
	public void write(Kryo kryo, Output output, java.sql.Date object) {
		// TODO Auto-generated method stub
		output.writeLong(object.getTime());
	}

	@Override
	public java.sql.Date read(Kryo kryo, Input input, Class<java.sql.Date> type) {
		// TODO Auto-generated method stub
		return new java.sql.Date(input.readLong());
	}

}
