package com.capgemini.ipl;


public class IPLAllRounder {
		private String player;
		private double battingAverage;
		private double bowlingAverage;
		private int runs;
		private int wickets;

		public IPLAllRounder(String player, double battingAverage, double bowlingAverage, int runs, int wickets) {
			super();
			this.player = player;
			this.battingAverage = battingAverage;
			this.bowlingAverage = bowlingAverage;
			this.runs = runs;
			this.wickets = wickets;
		}

		public String getPlayer() {
			return player;
		}

		public double getBattingAverage() {
			return battingAverage;
		}

		public double getBowlingAverage() {
			return bowlingAverage;
		}

		public int getRuns() {
			return runs;
		}

		public int getWickets() {
			return wickets;
		}

		public double getPerformanceByAverage() {
			return getBattingAverage()-getBowlingAverage();
		}

		public double getPerformanceByRunsAndWickets() {
			return (this.runs*this.wickets);
		}
		@Override
		public String toString() {
			return "IPLAllRounder [player=" + player + ", battingAverage=" + battingAverage + ", bowlingAverage="
					+ bowlingAverage + ", runs=" + runs + ", wickets=" + wickets + "]";
		}
}