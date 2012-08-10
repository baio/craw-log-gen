import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class LogParser {
	
	
	public KeyValue<String, String> ParseLine(String Line)
	{
		JSONObject json = null;
	    
	    String line = Line;
		
		//string is a json object, parse it
		try {
			json = (JSONObject)new JSONParser().parse(line);
		} catch (ParseException e) {
			return null;
		}
	    
		//get all map fields from json
	    String id = (String) json.get("id");
	    
	    if (id != null)
	    {   
	    	int level = ((Long)json.get("level")).intValue();
	    	
	    	String code = (String)json.get("code");
	    	
	    	String val = null;	    	 
	    	
	    	if (level > 30)
	    	{
	    		//errors
	    		
	    		String errCode = (String)json.get("code");
	    		
	    		switch(level)
	    		{
		    		case 40:
		    			val = "warn " + errCode;
		    			break;
		    		case 50:
		    			val = "err " + errCode;
		    			break;
		    		case 60:
		    			val = "fatal " + errCode;
		    			break;    		
	    		}    	    		
	    	}
	    	else if (level == 30) 
	    	{    
	    		//info
	    		
	    		if (code.equals("start"))
	    		{
	    			JSONObject parserCtx = (JSONObject) json.get("parserCtx");
	    			
					val = "start " + json.get("craw_id") + " " + json.get("time") + " " + parserCtx.get("countryIter") 
							+ " " + parserCtx.get("dateIter") + " " + parserCtx.get("page");				
	    		}	    		
	    		else if (code.equals("complete"))
	    		{
	    			val = "complete " + json.get("time");
	    		}
	    		else if (code.equals("request"))
	    		{	    		
	    			val = "request";
	    		}
	    		else if (code.equals("response"))
	    		{
	    			val = "response";
	    		}
	    		else if (code.equals("found"))
	    		{
	    			val = "found " + json.get("count");
	    		}	    			
	    	}
	    	
	    	if (val != null)
	    	{
	    		    	
	    		return new KeyValue<String, String>(id, val);	    		
	    	}    	    		    		
	    }
	    
		return null;		
	}

}
