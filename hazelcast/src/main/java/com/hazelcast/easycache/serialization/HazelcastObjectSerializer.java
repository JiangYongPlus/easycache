package com.hazelcast.easycache.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.nio.serialization.StreamSerializer;

public class HazelcastObjectSerializer implements StreamSerializer<HazelcastObject> {
	
	private static boolean localOptimization = false;
	
	@Override
	public int getTypeId() {
		if (!localOptimization) {
			return SerializationConstants.EASYCACHE_HAZELCAST_OBJECT_ID;
		}
		return SerializationConstants.EASYCACHE_HAZELCAST_OBJECT_ID + 1;
	}

	public static void setLocalOptimization(boolean localOptimization) {
		HazelcastObjectSerializer.localOptimization = localOptimization;
	}
	
	public static boolean getPstOptimization() {
		return HazelcastObjectSerializer.localOptimization;
	}
	
	@Override
	public void destroy() {
	}

	@Override
	public void write(ObjectDataOutput out, HazelcastObject object) throws IOException {
		Kryo kryo = new Kryo();
		kryo.addDefaultSerializer(java.sql.Date.class, KryoDateSerializer.class);
		kryo.addDefaultSerializer(java.sql.Timestamp.class, KryoTimeStampSerializer.class);
		
		Output output = new Output((OutputStream)out);
		kryo.writeClassAndObject(output, object);
		output.flush();
	}

	@Override
	public HazelcastObject read(ObjectDataInput in) throws IOException {
		Kryo kryo = new Kryo();
		kryo.addDefaultSerializer(java.sql.Date.class, KryoDateSerializer.class);
		kryo.addDefaultSerializer(java.sql.Timestamp.class, KryoTimeStampSerializer.class);
		
		Input input = new Input((InputStream)in);
		return (HazelcastObject) kryo.readClassAndObject(input);
	}
}
