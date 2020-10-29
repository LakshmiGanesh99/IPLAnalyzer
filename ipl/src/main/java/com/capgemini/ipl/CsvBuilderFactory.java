package com.capgemini.ipl;

public class CsvBuilderFactory {
	public static ICsvBuilder createBuilder() {
		return new OpenCsvBuilder();
	}
}