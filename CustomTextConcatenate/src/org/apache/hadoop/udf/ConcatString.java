package org.apache.hadoop.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

@Description(
		name = "concate_ws",
		value = "_FUNC_(str,array<str>) - Concatenate array of string to one string with delimiter",
		extended = "Example:\n" +
		"  > SELECT concate('SEP',COLLECT_SET(bank_code)) FROM tbl_banks;\n" +
		"BOFASEPCITISEPJPMC"
		)
public class ConcatString extends UDF {
	public Text evaluate(Text delimiter, ArrayList<Text> listHeadOffices) {
		String to_value = "";
		int counter = 0;
		if (delimiter != null && listHeadOffices != null) {
			try {
				for(Text headOffice : listHeadOffices) {
					to_value += headOffice.toString();
					
					// If the counter is less than ArrayList length, then append delimiter
					if(counter < listHeadOffices.size()) {
						to_value += delimiter.toString();
						counter++;
					}
				}				
			}
			catch(Exception ex) {
				to_value = delimiter.toString();
			}
		}		
		return (new Text(to_value));
	}
}
