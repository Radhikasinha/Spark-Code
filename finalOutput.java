package Maincode;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.input.TeeInputStream;

public class finalOutput {
	public static void main(String[] args) throws IOException {
		FileOutputStream file = new FileOutputStream("test.txt");
	//	TeePrintStream tee = new TeeInputStream(file, System.out);
		// Reading file

		BufferedReader br = new BufferedReader(new FileReader("part-r-00000"));

		int DateMaxAccidents = 0, BMaxAccidents = 0, ZMaxAccidents = 0, VTypeMaxAccidents = 0, YearPIMaxAccidents = 0,

				YearPKMaxAccidents = 0, cycleMaxAccidents = 0, motorMaxAccidents = 0;

		String Date_Accident = new String();

		String Borough_Accident = new String();

		String Zip_Accident = new String();

		String VehicleType_Accident = new String();

		String YearPpInured_Accident = new String();

		String YearPpKilled_Accident = new String();

		String cyclist_Accident = new String();

		String motorists_Accident = new String();

		String str;



		while ((str = br.readLine()) != null) {

			if (str.equals(""))

				continue;

			if (str.startsWith("Date_")) {

				String origRow = str.substring(5);

				int space = Math.max(origRow.lastIndexOf((char) 9), origRow.lastIndexOf((char) 32));

				String name = origRow.substring(0, space);

				int val = Integer.parseInt(origRow.substring(space + 1, origRow.length()));

				if (val > DateMaxAccidents) {

					DateMaxAccidents = val;

					Date_Accident = name;

				}



			} else if (str.startsWith("Boro_")) {

				String origRow = str.substring(5);

				int space = Math.max(origRow.lastIndexOf((char) 9), origRow.lastIndexOf((char) 32));

				String name = origRow.substring(0, space);

				int val = Integer.parseInt(origRow.substring(space + 1, origRow.length()));

				if (val > BMaxAccidents) {

					BMaxAccidents = val;

					Borough_Accident = name;

				}

			} else if (str.startsWith("ZipC_")) {

				String origRow = str.substring(5);

				int space = Math.max(origRow.lastIndexOf((char) 9), origRow.lastIndexOf((char) 32));

				String name = origRow.substring(0, space);

				int val = Integer.parseInt(origRow.substring(space + 1, origRow.length()));

				if (val > ZMaxAccidents ) {

					ZMaxAccidents = val;

					Zip_Accident = name;

				}

			} else if (str.startsWith("Vehi_")) {

				String origRow = str.substring(5);

				int space = Math.max(origRow.lastIndexOf((char) 9), origRow.lastIndexOf((char) 32));

				String name = origRow.substring(0, space);

				int val = Integer.parseInt(origRow.substring(space + 1, origRow.length()));

				if (val > VTypeMaxAccidents) {

					VTypeMaxAccidents = val;

					VehicleType_Accident = name;

				}

			} else if (str.startsWith("ppdi_")) {

				String origRow = str.substring(5);

				int space = Math.max(origRow.lastIndexOf((char) 9), origRow.lastIndexOf((char) 32));

				String name = origRow.substring(0, space);

				int val = Integer.parseInt(origRow.substring(space + 1, origRow.length()));

				if (val > YearPIMaxAccidents) {

					YearPIMaxAccidents = val;

					YearPpInured_Accident = name;

				}

			} else if (str.startsWith("ppdk_")) {

				String origRow = str.substring(5);

				int space = Math.max(origRow.lastIndexOf((char) 9), origRow.lastIndexOf((char) 32));

				String name = origRow.substring(0, space);

				int val = Integer.parseInt(origRow.substring(space + 1, origRow.length()));

				if (val > YearPKMaxAccidents) {

					YearPKMaxAccidents = val;

					YearPpKilled_Accident = name;

				}

			} else if (str.startsWith("cyik_")) {

				String origRow = str.substring(5);

				int space = Math.max(origRow.lastIndexOf((char) 9), origRow.lastIndexOf((char) 32));

				String name = origRow.substring(0, space);

				int val = Integer.parseInt(origRow.substring(space + 1, origRow.length()));

				if (val > cycleMaxAccidents) {

					cycleMaxAccidents = val;

					cyclist_Accident = name;

				}

			} else if (str.startsWith("moik_")) {

				String origRow = str.substring(5);

				int space = Math.max(origRow.lastIndexOf((char) 9), origRow.lastIndexOf((char) 32));

				String name = origRow.substring(0, space);

				int val = Integer.parseInt(origRow.substring(space + 1, origRow.length()));

				if (val > motorMaxAccidents) {

					motorMaxAccidents = val;

					motorists_Accident = name;

				}

			}

		}

		br.close();
		File file1 = new File("out.txt"); //File where final output will be saved
		FileOutputStream fos = new FileOutputStream(file1);
		PrintStream ps = new PrintStream(fos);
		System.setOut(ps);
		System.out.println("This goes to out.txt");

		System.out.println(

				"1. Date on which maximum number of accidents took place:  " + Date_Accident + "  " + DateMaxAccidents);

		System.out.println(

				"2. Borough with maximum count of accident fatality:  " + Borough_Accident + " " + BMaxAccidents);

		System.out.println("3. Zip with maximum count of accident fatality:  " + Zip_Accident + " " + ZMaxAccidents);

		System.out.println("4. vehicle type involved in maximum accidents:  " + VehicleType_Accident + " "

				+ VTypeMaxAccidents);

		System.out.println("5. Year with maximum Number Of Persons and Pedestrians Injured:  "

				+ YearPpInured_Accident + " " + YearPIMaxAccidents);

		System.out.println("6. Year with maximum Number Of Persons and Pedestrians Killed:  "

				+ YearPpKilled_Accident + " " + YearPKMaxAccidents);

		System.out.println("7. Year with maximum Number Of Cyclist Injured and Killed:  " + cyclist_Accident + " "

				+ cycleMaxAccidents);

		System.out.println("8. Year with maximum Number Of Motorist Injured and Killed:  " + motorists_Accident

				+ " " + motorMaxAccidents);

	}



}