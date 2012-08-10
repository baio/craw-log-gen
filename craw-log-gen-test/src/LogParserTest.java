import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.junit.Test;

public class LogParserTest {

	// @Test
	public void map_data_sample_1() {

		try {
			// Open the file that is the first
			// command line parameter
			FileInputStream fstream = new FileInputStream(
					"data/mapper-input-1.txt");
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			// Read File Line By Line

			LogParser logParser = new LogParser();

			while ((strLine = br.readLine()) != null) {
				// Print the content on the console
				// System.out.println (strLine);
				KeyValue<String, String> kv = logParser.ParseLine(strLine);

				assertNotNull(kv);
				assertNotNull(kv.Key);
				assertNotNull(kv.Val);

				System.out.println(kv.Key + " : " + kv.Val);

			}
			// Close the input stream
			in.close();

		} catch (Exception e) {// Catch exception if any

			fail();
		}
	}

	@Test
	public void reduce_data_sample_1() {

		try {
			// Open the file that is the first
			// command line parameter
			FileInputStream fstream = new FileInputStream(
					"data/reducer-input-1.txt");
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			// Read File Line By Line

			LogFormatter logFormatter = new LogFormatter();

			while ((strLine = br.readLine()) != null) {
				// Print the content on the console
				// System.out.println (strLine);

				String[] spts = strLine.split(":", 2);

				LogFormatter.FormatResult formatResult = logFormatter
						.FormatStringInput(spts[0].trim(), spts[1].trim());

				assertNotNull(formatResult);

			}
			// Close the input stream
			in.close();

		} catch (Exception e) {// Catch exception if any

			fail();
		}
	}

}
