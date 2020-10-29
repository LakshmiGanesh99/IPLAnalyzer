package com.capgemini.ipl;

public class IPLAnalyserException extends Exception {
	IPLAnalyserExceptionType exceptionType;
	public IPLAnalyserException(String message, IPLAnalyserExceptionType exceptionType) {
		super(message);
		this.exceptionType=exceptionType;
	}
}

enum IPLAnalyserExceptionType{
	CENSUS_FILE_PROBLEM, INCORRECT_TYPE,SOME_OTHER_ERRORS ,PARSE_ERROR
}
