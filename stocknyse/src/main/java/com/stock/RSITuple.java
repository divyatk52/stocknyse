package com.stock;
import java.io.Serializable;

public class RSITuple implements Serializable {
	/**This class is used to get and set close ,avgGain,avgLoss and count .
	 * 
	 */
	private static final long serialVersionUID = -2320967455791630038L;
	
	double close;
	double avgGain;
	double avgLoss;
	int count;
	//constructors
	public RSITuple(double close, double avgGain, double avgLoss, int count) {
		super();
		this.close = close;
		this.avgGain = avgGain;
		this.avgLoss = avgLoss;
		this.count = count;
	}
	public RSITuple() {
		super();
		this.close = 0.0;
		this.avgGain = 0.0;
		this.avgLoss = 0.0;
		this.count = 0;
	}
	//Getter and Setters
	public double getClose() {
		return close;
	}
	public void setClose(double close) {
		this.close = close;
	}
	public double getAvgGain() {
		return avgGain;
	}
	public void setAvgGain(double avgGain) {
		this.avgGain = avgGain;
	}
	public double getAvgLoss() {
		return avgLoss;
	}
	public void setAvgLoss(double avgLoss) {
		this.avgLoss = avgLoss;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	@Override
	public String toString() {
		Double rs = 0.0;
		//if the count is 10 or more rsi is calculated else rsi is kept 0.
		if(count>=10) {
		if (avgGain > 0 && avgLoss > 0 ) {
			rs = avgGain / avgLoss;

			} else if (avgGain == 0) {
			rs = 0d;
			} else if (avgLoss == 0) {
			rs = 100d;
			}
			//System.out.println("close:"+close+"avgGain:"+avgGain+"avgLoss:"+avgLoss+"rs:"+rs+"rsi:"+(100 - 100 / (1 + rs))+"count:"+count);
			return String.valueOf(100 - 100 / (1 + rs));
	}else 
		return "close:"+close+"avgGain:"+avgGain+"avgLoss:"+avgLoss+"count:"+count+"rsi:"+0;
	}

}
