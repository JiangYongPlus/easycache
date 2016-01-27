package com.hazelcast.easycache.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoTimeStampSerializer extends Serializer<java.sql.Timestamp>{

	@Override
	public void write(Kryo kryo, Output output, java.sql.Timestamp object) {
		// TODO Auto-generated method stub
		output.writeLong(object.getTime());
	}

	@Override
	public java.sql.Timestamp read(Kryo kryo, Input input, Class<java.sql.Timestamp> type) {
		// TODO Auto-generated method stub
		return new java.sql.Timestamp(input.readLong());
	}
}
