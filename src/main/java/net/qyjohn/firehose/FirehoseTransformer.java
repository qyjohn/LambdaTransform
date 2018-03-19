/*
This is a (quick and dirty) demo for Kinesis Firehose Transformation via Lambda in Java.

In short, Firehose expects the following data structure. This can be achieved by returning POJO. Please refer to the
following AWS documentation on using POJO to handle input and output in Lambda.

https://docs.aws.amazon.com/lambda/latest/dg/java-handler-io-type-pojo.html

{
    "records": [
        {
            "recordId": "...",
            "result": "Ok",
            "data": "..."
        },
        {
            "recordId": "...",
            "result": "Ok",
            "data": "..."
        },
        {
            "recordId": "...",
            "result": "Ok",
            "data": "..."
        }
    ]
}

Below is a better tutorial on POJO.

https://www.ibm.com/support/knowledgecenter/en/SS7JFU_8.5.5/com.ibm.websphere.express.doc/ae/twbs_jaxrs_jsoncontent_pojo.html
*/

package net.qyjohn.firehose;

import java.util.List;
import java.util.ArrayList;
import java.util.Base64;
import java.nio.charset.StandardCharsets;
import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.lambda.runtime.events.*; 

public class FirehoseTransformer implements RequestHandler<KinesisFirehoseEvent, ResponseClass>
{

	// This is the Lambda function handler
	
	public ResponseClass handleRequest(KinesisFirehoseEvent event, Context context)
	{
		ResponseClass response = new ResponseClass();
		List<KinesisFirehoseEvent.Record> records = event.getRecords();
		for(KinesisFirehoseEvent.Record rec : records)
		{
			String recordId = rec.getRecordId();
			String status = "Ok";
			String data = StandardCharsets.UTF_8.decode(rec.getData()).toString();
			ResponseRecord resp = new ResponseRecord(recordId, status, data);
			response.add(resp);
		}
		return response;		
	}
}


class ResponseClass
{
	ArrayList<ResponseRecord> records = new ArrayList<ResponseRecord>();
	
	public ArrayList<ResponseRecord> getRecords()
	{
		return records;
	}

	public void add(ResponseRecord record)
	{
		records.add(record);
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
	
	public void setRecordId(String recordId)
	{
		this.recordId = recordId;
	}
	
	public String getRecordId()
	{
		return recordId;
	}
	
	public void setResult(String result)
	{
		this.result = result;
	}
	
	public String getResult()
	{
		return result;
	}
	
	public void setData(String data)
	{
		this.data = Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));
	}
	
	public String getData()
	{
		return data;
	}
}

