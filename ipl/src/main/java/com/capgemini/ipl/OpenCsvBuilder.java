package com.capgemini.ipl;

import java.io.Reader;
import java.util.List;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

public class OpenCsvBuilder implements ICsvBuilder{
	
	public <T> CsvToBean<T> getCsvToBean(Reader reader, Class<T> csvBindedClass) throws RuntimeException {
		CsvToBeanBuilder<T> csvToBeanBuilder = new CsvToBeanBuilder<>(reader);
		csvToBeanBuilder.withType(csvBindedClass);
		csvToBeanBuilder.withIgnoreLeadingWhiteSpace(true);
		CsvToBean<T> csvToBean = csvToBeanBuilder.build();
		return csvToBean;
	}
	
	public <T> List<T> getListFromCsv(Reader reader, Class<T> csvBindedClass) throws IPLAnalyserException {
		try {
			
			CsvToBean<T> csvToBean = this.getCsvToBean(reader, csvBindedClass);
			List<T> censusList = csvToBean.parse();
			return censusList;
		} 
		catch (IllegalStateException e) {

			throw new IPLAnalyserException("Parsing Error", IPLAnalyserExceptionType.PARSE_ERROR);
		}
		catch (RuntimeException e) {

			throw new IPLAnalyserException("Wrong Delimiter or Header", IPLAnalyserExceptionType.SOME_OTHER_ERRORS);
		}
		
	}
}
