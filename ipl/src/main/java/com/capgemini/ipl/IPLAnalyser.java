package com.capgemini.ipl;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import com.google.gson.Gson;

public class IPLAnalyser {

	public int loadBatsmenData(String csvFilePath) throws IPLAnalyserException {
		if (!(csvFilePath.matches(".*\\.csv$")))
			throw new IPLAnalyserException("Incorrect Type", IPLAnalyserExceptionType.INCORRECT_TYPE);
		try (Reader reader = Files.newBufferedReader(Paths.get(csvFilePath));) {

			ICsvBuilder csvBuilder = CsvBuilderFactory.createBuilder();
			List<CSVIPLRecords> batsmenList = csvBuilder.getListFromCsv(reader, CSVIPLRecords.class);
			return batsmenList.size();

		} catch (IOException e) {
			throw new IPLAnalyserException("Incorrect CSV File", IPLAnalyserExceptionType.CENSUS_FILE_PROBLEM);
		} catch (RuntimeException e) {
			throw new IPLAnalyserException("Wrong Delimiter or Header", IPLAnalyserExceptionType.SOME_OTHER_ERRORS);
		}
	}

	// UC1 sorting of cricketer with top batting averages && UC5 sorting the cricketers who had best averages with good striking

	public String getSortedBatsmenListOnBattingAverage(String csvFilePath) throws IPLAnalyserException {
		try (Reader reader = Files.newBufferedReader(Paths.get(csvFilePath));) {
			ICsvBuilder csvBuilder = CsvBuilderFactory.createBuilder();
			List<CSVIPLRecords> batsmenList = csvBuilder.getListFromCsv(reader, CSVIPLRecords.class);
			Function<CSVIPLRecords, Double> batsmanEntity = record -> record.average;
			Comparator<CSVIPLRecords> censusComparator = Comparator.comparing(batsmanEntity);
			this.sortBatsmenList(batsmenList, censusComparator);
			String sortedStateCensusToJson = new Gson().toJson(batsmenList);
			return sortedStateCensusToJson;
		} catch (IOException e) {
			throw new IPLAnalyserException("Incorrect CSV File", IPLAnalyserExceptionType.CENSUS_FILE_PROBLEM);
		}
	}
	// UC2 sorting of cricketers to know the top striking rate of batsman  && UC4 to 
	// sort cricketeres with best striking rate and top 6s and 4s

	public String getSortedBatsmenListOnTopStrikingRates(String csvFilePath) throws IPLAnalyserException {
		try (Reader reader = Files.newBufferedReader(Paths.get(csvFilePath));) {
			ICsvBuilder csvBuilder = CsvBuilderFactory.createBuilder();
			List<CSVIPLRecords> batsmenList = csvBuilder.getListFromCsv(reader, CSVIPLRecords.class);
			Function<CSVIPLRecords, Double> batsmanEntity = record -> record.strikeRate;
			Comparator<CSVIPLRecords> censusComparator = Comparator.comparing(batsmanEntity);
			this.sortBatsmenList(batsmenList, censusComparator);
			String sortedStateCensusToJson = new Gson().toJson(batsmenList);
			return sortedStateCensusToJson;
		} catch (IOException e) {
			throw new IPLAnalyserException("Incorrect CSV File", IPLAnalyserExceptionType.CENSUS_FILE_PROBLEM);
		}
	}

	// UC3 sorting of cricketers who hit maximum 6s and 4s

	public String getSortedBatsmenListOnMostSixes(String csvFilePath) throws IPLAnalyserException {
		try (Reader reader = Files.newBufferedReader(Paths.get(csvFilePath));) {
			ICsvBuilder csvBuilder = CsvBuilderFactory.createBuilder();
			List<CSVIPLRecords> batsmenList = csvBuilder.getListFromCsv(reader, CSVIPLRecords.class);
			Function<CSVIPLRecords, Integer> batsmanEntity = record -> record.sixes;
			Comparator<CSVIPLRecords> censusComparator = Comparator.comparing(batsmanEntity);
			this.sortBatsmenList(batsmenList, censusComparator);
			String sortedStateCensusToJson = new Gson().toJson(batsmenList);
			return sortedStateCensusToJson;
		} catch (IOException e) {
			throw new IPLAnalyserException("Incorrect CSV File", IPLAnalyserExceptionType.CENSUS_FILE_PROBLEM);
		}
	}

	public String getSortedBatsmenListOnMostFours(String csvFilePath) throws IPLAnalyserException {
		try (Reader reader = Files.newBufferedReader(Paths.get(csvFilePath));) {
			ICsvBuilder csvBuilder = CsvBuilderFactory.createBuilder();
			List<CSVIPLRecords> batsmenList = csvBuilder.getListFromCsv(reader, CSVIPLRecords.class);
			Function<CSVIPLRecords, Integer> batsmanEntity = record -> record.fours;
			Comparator<CSVIPLRecords> censusComparator = Comparator.comparing(batsmanEntity);
			this.sortBatsmenList(batsmenList, censusComparator);
			String sortedStateCensusToJson = new Gson().toJson(batsmenList);
			return sortedStateCensusToJson;
		} catch (IOException e) {
			throw new IPLAnalyserException("Incorrect CSV File", IPLAnalyserExceptionType.CENSUS_FILE_PROBLEM);
		}
	}

	public void sortBatsmenList(List<CSVIPLRecords> batsmenList, Comparator<CSVIPLRecords> censusComparator) {
		for (int i = 0; i < batsmenList.size() - 1; i++) {
			for (int j = 0; j < batsmenList.size() - i - 1; j++) {
				CSVIPLRecords sortKey1 = batsmenList.get(j);
				CSVIPLRecords sortKey2 = batsmenList.get(j + 1);
				if (censusComparator.compare(sortKey1, sortKey2) < 0) {
					batsmenList.set(j, sortKey2);
					batsmenList.set(j + 1, sortKey1);
				}
			}
		}
	}
}
