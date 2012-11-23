package backtype.storm.security.serialization;

import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.BlowfishSerializer;

import backtype.storm.serialization.types.ListDelegateSerializer;
import backtype.storm.utils.ListDelegate;

public class BlowfishTupleSerializer extends Serializer<ListDelegate> {
    /**
     * The secret key (if any) for data encryption by blowfish payload serialization factory (BlowfishSerializationFactory). 
     * You should use in via "storm jar" parameter:
     */
    public static String SECRET_KEY = "topology.tuple.serializer.blowfish.key";
    private static final Logger LOG = Logger.getLogger(BlowfishSerializer.class);
    private BlowfishSerializer _serializer;

    public BlowfishTupleSerializer(Kryo kryo, Map storm_conf) {
	String encryption_key = "undefined";
	try {
	    encryption_key = (String)storm_conf.get(SECRET_KEY);
	    LOG.debug("Blowfish serializer being constructed ...");
	    if (encryption_key == null) {
		LOG.error("Encryption key not specified");
		throw new RuntimeException("Blowfish encryption key not specified");
	    }
	    byte[] bytes =  Hex.decodeHex(encryption_key.toCharArray());
	    _serializer = new BlowfishSerializer(new ListDelegateSerializer(), bytes);
	} catch (org.apache.commons.codec.DecoderException ex) {
	    LOG.error("Invalid encryption key:"+encryption_key);
	    throw new RuntimeException("Blowfish encryption key invalid");
	}
    }

    @Override
    public void write(Kryo kryo, Output output, ListDelegate object) {
	_serializer.write(kryo, output, object);
    }

    @Override
    public ListDelegate read(Kryo kryo, Input input, Class<ListDelegate> type) {
	return (ListDelegate)_serializer.read(kryo, input, type);
    }
}
