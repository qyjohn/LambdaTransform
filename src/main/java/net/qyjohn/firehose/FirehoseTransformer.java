package net.qyjohn.firehose;

import java.util.LinkedList;
import java.util.Base64;
import java.nio.charset.StandardCharsets;
import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.lambda.runtime.events.*; 

public class FirehoseTransformer
{

	public LinkedList<ResponseRecord> handler(KinesisFirehoseEvent event)
	{
		LinkedList<ResponseRecord> records = new LinkedList<ResponseRecord>();
		return records;
	}

}

class ResponseRecord
{
	String recordId;
	String result;
	String data;

	public ResponseRecord(String recordId, String result, String data)
	{
		this.recordId = recordId;
		this.result = result;
		this.data = Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));
	}
}

