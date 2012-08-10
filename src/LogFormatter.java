import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

class LogFormatter {
	
	class FormatResult
	{
		String id = null;
		String crawId = null;
		ArrayList<String> steps = new ArrayList<String>();
	  	Date start = null;
	  	Date complete = null;
	  	int found = 0;
	  	
	  	int warns = 0;
	  	int errs = 0;
	  	int fatals = 0;
	  	
	  	int country = 0;
	  	int page = 0;
	  	Date date = null;  				
	}
	
	private Date parseDate(String DateStr) throws java.text.ParseException
	{
		if (DateStr != null)
		{
			java.text.DateFormat dateFormat = new java.text.SimpleDateFormat("YYY-MM-DD'T'HH:mm:ss.SSS'Z'");
			
			return dateFormat.parse(DateStr);			
		}
		else
		{
			return null;			
		}		
	}
	       
	private FormatResult Format(Text Key, Iterable<Text> values) throws java.text.ParseException
    {    	   
		FormatResult result = new FormatResult();
	  		  
	    for(final Text line : values) {
	    	    	        		    	
	    	String [] vals = line.toString().split(" ");
	    	
	    	String stp = vals[0]; 
	    	
	    	if (stp.equals("start"))
	    	{
	    		result.steps.add("start");
	    		result.crawId = (String)vals[1];
	    		result.start = this.parseDate(vals[2]);
	    		result.country = Integer.parseInt(vals[3]);
	    		result.date = parseDate(vals[4]);
	    		result.page = Integer.parseInt(vals[5]);
	    	}
	    	else if (stp.equals("complete"))
	    	{
	    		result.steps.add("complete");
	    		result.complete = parseDate(vals[1]);
	    	}else if (stp.equals("request"))
	    	{
	    		result.steps.add("rqeuest");
	    	}
	    	else if (stp.equals("respond"))
	    	{	    		
	    		result.steps.add("respond");
	    	}	    		
	    	else if (stp.equals("found"))
	    	{
	    		result.steps.add("found");
	    		result.found = Integer.parseInt(vals[1]);
	    	}
	    	else if (stp.equals("warn"))
	    	{
	    		result.warns++;
	    	}
	    	else if (stp.equals("end"))
	    	{
	    		result.errs++;
	    	}
	    	else if (stp.equals("fatal"))
	    	{
	    		result.fatals++;    		   
	    	}	           
	      }
	    	    
	    result.id = Key.toString();
	   	  
	    return result;
	    	   	    	    	  	   
    }
	
		
	private BSONObject AsBSON(FormatResult FormatResult) throws java.text.ParseException
	{
		
    	BSONObject bsonObject = new BasicBSONObject();
    	bsonObject.put("_id", FormatResult.id);
    	bsonObject.put("steps", FormatResult.steps.toArray());
    	bsonObject.put("start", FormatResult.start);
    	bsonObject.put("complete", FormatResult.complete);    
    	bsonObject.put("found", FormatResult.found);
    	bsonObject.put("country", FormatResult.country);
    	bsonObject.put("page", FormatResult.page);
    	bsonObject.put("date", FormatResult.date);
    	bsonObject.put("warns", FormatResult.warns);
    	bsonObject.put("errs", FormatResult.errs);
    	bsonObject.put("fatals", FormatResult.fatals);
    	
    	return bsonObject;
	}
		
	BSONObject AsBSON(Text Key, Iterable<Text> values) throws java.text.ParseException
	{
		return AsBSON(Format(Key, values));
	}
	

	FormatResult FormatStringInput(String Key, String values)
	{
		ArrayList<Text> iter = new ArrayList<Text>(); 
		
		iter.add(new Text(values));
		
		try {
			return Format(new Text(Key), (Iterable<Text>) iter);
		} catch (ParseException e) {
			return null;
		}		
	}
}
