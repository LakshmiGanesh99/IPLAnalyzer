package com.capgemini.ipl;


import com.opencsv.bean.CsvBindByName;

public class CSVIPLBowlersRecords {
	@CsvBindByName(column = "POS")
	public int position;
	@CsvBindByName(column = "PLAYER")
	public String player;
	@CsvBindByName(column = "MAT")
	public int match;
	@CsvBindByName(column = "Inns")
	public int innings;
	@CsvBindByName(column = "Ov")
	public double overs;
	@CsvBindByName(column = "Runs")
	public int runs;
	@CsvBindByName(column = "Wkts")
	public int wickets;
	@CsvBindByName(column = "BBI")
	public int bbi;
	@CsvBindByName(column = "Avg")
	public double average;
	@CsvBindByName(column = "Econ")
	public double economy;
	@CsvBindByName(column = "SR")
	public double strikeRate;
	@CsvBindByName(column = "4w")
	public int fourWktHaul;
	@CsvBindByName(column = "5w")
	public int fiveWktHaul;

	@Override
	public String toString() {
		return "CSVIPLBowlersRecords [position=" + position + ", player=" + player + ", match=" + match + ", innings="
				+ innings + ", overs=" + overs + ", runs=" + runs + ", wickets=" + wickets + ", bbi=" + bbi
				+ ", average=" + average + ", economy=" + economy + ", strikeRate=" + strikeRate + ", fourWktHaul="
				+ fourWktHaul + ", fiveWktHaul=" + fiveWktHaul + "]";
	}}
