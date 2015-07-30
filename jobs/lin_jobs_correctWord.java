// impor java for readers and array list
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;

// import for udfs
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class lin_jobs_correctWord extends EvalFunc<String>{
	
	HashSet<String> dictionary = null;

	public List<String> getCacheFiles() { 
        List<String> list = new ArrayList<String>(1); 
        list.add("dictionary.txt#dic"); 
        return list; 
    } 

    public static boolean isNumber(String str) {
	    try 
	    {
	        Double.parseDouble(str);
	        return true;
	    } 
	    
	    catch (NumberFormatException e) 
	    {
	        return false;
	    }
	}
	
	public String exec(Tuple input) throws IOException{

		if (input == null || input.size() == 0)
 			return null;
		try
		{	
			// Initialized if not done so
			if (dictionary==null){

				dictionary =  new HashSet<String>();

				// Open the file as a local file.
				FileReader fr = new FileReader("./dic");
				BufferedReader d = new BufferedReader(fr);
				String line = "";
				while ((line = d.readLine()) != null) 
				{
					dictionary.add(line);

				} // end while

				fr.close();

			} // end if
		
			// initialize variables
			String minWord = "";
			String word = (String)input.get(0);
	        int minDist = 1000;
	        int dist = 1000;
	        Levenshtein lev = new Levenshtein();

	        // check blank word
	        if (word.equals(""))
	        {
	        	return minWord;
	        }

	        // check word is number
	        else if (isNumber(word))
	        {
	        	return word; 	
	        }
	        
	        // check word is in dictionary (not misspelled)
	        // Levenshtein Distance = 0
	        else if(dictionary.contains(word))
	        {
	        	return word;	
	        }

	        // correct word
	        else
	        {	
	        	for (String wordDic : dictionary )
	            {	
	        		dist = lev.getLevenshteinDistance(word, wordDic);

	        		// return the first word found with Levenshtein Distance = 1 if any
	     			// which is the lowest possible value (integer) after zero
	        		// note it cannot be zero since this was checked in previous else if statement
	        		if(dist == 1)
	        		{
        				return wordDic;	
	        		}

	        		else if(dist < minDist)
	        		{
	        			minDist = dist;
	        			minWord = wordDic;
	        		}
	        		
	            } // end for
	        	
	        } // end else

			return minWord;

		} // end try

		catch(Exception e)
		{
			throw new IOException("Caught exception processing input row ", e);

		} // end catch

	} // end exec
	
} // end class