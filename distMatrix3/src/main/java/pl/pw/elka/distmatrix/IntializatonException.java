package pl.pw.elka.distmatrix;

public class IntializatonException extends Exception {

	public String getMessage() {
		String message = "Incorrect folder structure, please ensure existing following in directory specified \n";
		message += "input                - mandatory directory \n";
		message += "regexform/regexform  - optional directory/file \n";
		message += "          sample: [-+.^:,~!@#$%^&*(){};?<>=\\[\\]]|\\d|\\s \n";
		message += "forbwords/forbwords  - optional directory/file \n";
		message += "          sample:I \n";
		message += "                 am \n";

		return message;
	}
}
