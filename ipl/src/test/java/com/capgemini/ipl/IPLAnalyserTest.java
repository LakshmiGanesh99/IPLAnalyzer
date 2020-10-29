package com.capgemini.ipl;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.gson.Gson;

public class IPLAnalyserTest {
	public static final String RIGHT_BATSMEN_CSV = "src/resources/IPL2019FactsheetMostRuns.csv";
	public static final String WRONG_BATSMEN_CSV = "src/resources/IPL2019FactsheetMostRunssdd.csv";
	public static final String WRONGTYPE_BATSMEN_CSV = "src/resources/IPL2019FactsheetMostRuns.pdf";
	public static final String RIGHT_BOWLERS_CSV = "src/resources/IPL2019FactsheetMostWkts.csv";
	public static final String WRONG_BOWLERS_CSV = "src/resources/IPL2019FactsheetMostwktsss.csv";
	public static final String WRONGTYPE_BOWLERS_CSV = "src/resources/IPL2019FactsheetMostWkts.pdf";
	
	@Test
	public void givenBatsmenDataCsv_ShouldReturnExactCount() {
		try {
			int recordsCount = new IPLAnalyser().loadBatsmenData(RIGHT_BATSMEN_CSV);
			assertEquals(100, recordsCount);
		}
		catch (IPLAnalyserException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void givenWrongBatsmenCsvFile_ShouldThrowIPLAnalyserExceptionOfTypeCensusFileProblem() {
		try {
			new IPLAnalyser().loadBatsmenData(WRONG_BATSMEN_CSV);
		}
		catch(IPLAnalyserException e) {
			assertEquals(IPLAnalyserExceptionType.CENSUS_FILE_PROBLEM, e.exceptionType);
		}
	}
	
	@Test
	public void givenWrongTypeBatsmenCsvFile_ShouldThrowIPLAnalyserExceptionOfTypeIncorrectType() {
		try {
			new IPLAnalyser().loadBatsmenData(WRONGTYPE_BATSMEN_CSV);
		}
		catch(IPLAnalyserException e) {
			assertEquals(IPLAnalyserExceptionType.INCORRECT_TYPE, e.exceptionType);
		}
	}
	
	@Test
	public void givenBowlersDataCsv_ShouldReturnExactCount() {
		try {
			int recordsCount = new IPLAnalyser().loadBatsmenData(RIGHT_BOWLERS_CSV);
			assertEquals(99, recordsCount);
		}
		catch (IPLAnalyserException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void givenWrongBowlersCsvFile_ShouldThrowIPLAnalyserExceptionOfTypeCensusFileProblem() {
		try {
			new IPLAnalyser().loadBatsmenData(WRONG_BOWLERS_CSV);
		}
		catch(IPLAnalyserException e) {
			assertEquals(IPLAnalyserExceptionType.CENSUS_FILE_PROBLEM, e.exceptionType);
		}
	}
	
	@Test
	public void givenWrongTypeBowlersCsvFile_ShouldThrowIPLAnalyserExceptionOfTypeIncorrectType() {
		try {
			new IPLAnalyser().loadBatsmenData(WRONGTYPE_BOWLERS_CSV);
		}
		catch(IPLAnalyserException e) {
			assertEquals(IPLAnalyserExceptionType.INCORRECT_TYPE, e.exceptionType);
		}
	}
	
	@Test
	public void givenSortedOnBattingAverageBatsmenList_ShouldReturnBestAveragedBatsman() {
		try {
			String sortedBatsmenJson = new IPLAnalyser().getSortedBatsmenListOnBattingAverage(RIGHT_BATSMEN_CSV);
			CSVIPLBatsmenRecords[] batsmenListCsv=new Gson().fromJson(sortedBatsmenJson, CSVIPLBatsmenRecords[].class);
			assertEquals("MS Dhoni", batsmenListCsv[0].player);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void givenSortedOnStrikingRatesBatsmenList_ShouldReturnTopStrikeRateBatsman() {
		try {
			String sortedBatsmenJson = new IPLAnalyser().getSortedBatsmenListOnTopStrikingRates(RIGHT_BATSMEN_CSV);
			CSVIPLBatsmenRecords[] batsmenListCsv=new Gson().fromJson(sortedBatsmenJson, CSVIPLBatsmenRecords[].class);
			assertEquals("Ishant Sharma", batsmenListCsv[0].player);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void givenSortedOnMostSixesBatsmenList_ShouldReturnWhoHitMostSixes() {
		try {
			String sortedBatsmenJson = new IPLAnalyser().getSortedBatsmenListOnMostSixes(RIGHT_BATSMEN_CSV);
			CSVIPLBatsmenRecords[] batsmenListCsv=new Gson().fromJson(sortedBatsmenJson, CSVIPLBatsmenRecords[].class);
			assertEquals("Andre Russell", batsmenListCsv[0].player);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void givenSortedOnMostFoursBatsmenList_ShouldReturnPlayerWhoHitMostFours() {
		try {
			String sortedBatsmenJson = new IPLAnalyser().getSortedBatsmenListOnMostFours(RIGHT_BATSMEN_CSV);
			CSVIPLBatsmenRecords[] batsmenListCsv=new Gson().fromJson(sortedBatsmenJson, CSVIPLBatsmenRecords[].class);
			assertEquals("Shikhar Dhawan", batsmenListCsv[0].player);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void givenSortedOnMaxRunsBatsmenList_ShouldReturnTopRunsScorer() {
		try {
			String sortedBatsmenJson = new IPLAnalyser().getSortedBatsmenListOnMaxRuns(RIGHT_BATSMEN_CSV);
			CSVIPLBatsmenRecords[] batsmenListCsv=new Gson().fromJson(sortedBatsmenJson, CSVIPLBatsmenRecords[].class);
			assertEquals("David Warner ", batsmenListCsv[0].player);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void givenSortedOnBowlingAverageBowlersList_ShouldReturnBestAveragedBowler() {
		try {
			String sortedBatsmenJson = new IPLAnalyser().getSortedBowlersListOnBowlingAverage(RIGHT_BOWLERS_CSV);
			CSVIPLBatsmenRecords[] batsmenListCsv=new Gson().fromJson(sortedBatsmenJson, CSVIPLBatsmenRecords[].class);
			assertEquals("Krishnappa Gowtham", batsmenListCsv[0].player);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
