package de.bschwering.edi_workshop;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.bschwering.edi_workshop.models.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Arrays;

public class JsonSerdes {
	
	private static final ObjectMapper mapper;
	
	static {
		mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public static Serde<Transaction> transactionSerde() {
		return Serdes.serdeFrom(
				(topic, data) -> writeValue(data), 
				(topic, data) -> readValue(data, Transaction.class));
	}

	public static Serde<Wallet> walletSerde() {
		return Serdes.serdeFrom(
				(topic, data) -> writeValue(data), 
				(topic, data) -> readValue(data, Wallet.class));
	}

	private static <T> T readValue(byte[] data, Class<T> clazz) {
		try {
			return mapper.readValue(data, clazz);
		} catch (IOException e) {
			return null;
		}
	}
	
	private static <T> byte[] writeValue(T data) {
		try {
			return mapper.writeValueAsBytes(data);
		} catch (IOException e) {
			return new byte[0];
		}
	}
	
	public static class JsonSerializer implements Serializer<Object> {
		public byte[] serialize(String topic, Object data) {
			return JsonSerdes.writeValue(data);
		}
	}

}
