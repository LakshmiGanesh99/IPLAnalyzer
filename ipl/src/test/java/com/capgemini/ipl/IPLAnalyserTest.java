package com.capgemini.ipl;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.gson.Gson;

public class IPLAnalyserTest {
	public static final String RIGHT_CENSUS_CSV = "/Users/lakshmiganesh/git/iplAnalyzer/ipl/src/resources/IPL2019FactsheetMostRuns.csv";
	public static final String WRONG_CENSUS_CSV = "/Users/lakshmiganesh/git/iplAnalyzer/ipl/src/resources/IPL2019FactsheetMostRuns.csv";
	public static final String WRONGTYPE_CENSUS_CSV = "/Users/lakshmiganesh/git/iplAnalyzer/ipl/src/resources/IPL2019FactsheetMostRuns.pdf";

	@Test
	public void givenBatsmenDataCsv_ShouldReturnExactCount() {
		try {
			int recordsCount = new IPLAnalyser().loadBatsmenData(RIGHT_CENSUS_CSV);
			assertEquals(100, recordsCount);
		}
		catch (IPLAnalyserException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void givenWrongCsvFile_ShouldThrowIPLAnalyserExceptionOfTypeCensusFileProblem() {
		try {
			new IPLAnalyser().loadBatsmenData(WRONG_CENSUS_CSV);
		}
		catch(IPLAnalyserException e) {
			assertEquals(IPLAnalyserExceptionType.CENSUS_FILE_PROBLEM, e.exceptionType);
		}
	}
	
	@Test
	public void givenWrongTypeCsvFile_ShouldThrowIPLAnalyserExceptionOfTypeIncorrectType() {
		try {
			new IPLAnalyser().loadBatsmenData(WRONGTYPE_CENSUS_CSV);
		}
		catch(IPLAnalyserException e) {
			assertEquals(IPLAnalyserExceptionType.INCORRECT_TYPE, e.exceptionType);
		}
	}
	
	@Test
	public void givenSortedOnBattingAverageBatsmenList_ShouldReturnBestAveragedBatsman() {
		try {
			String sortedBatsmenJson = new IPLAnalyser().getSortedBatsmenListOnBattingAverage(RIGHT_CENSUS_CSV);
			CSVIPLRecords[] batsmenListCsv=new Gson().fromJson(sortedBatsmenJson, CSVIPLRecords[].class);
			assertEquals("MS Dhoni", batsmenListCsv[0].player);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void givenSortedOnStrikingRatesBatsmenList_ShouldReturnTopStrikeRateBatsman() {
		try {
			String sortedBatsmenJson = new IPLAnalyser().getSortedBatsmenListOnTopStrikingRates(RIGHT_CENSUS_CSV);
			CSVIPLRecords[] batsmenListCsv=new Gson().fromJson(sortedBatsmenJson, CSVIPLRecords[].class);
			assertEquals("Ishant Sharma", batsmenListCsv[0].player);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}