package com.stock;
//PriceData is for better segregation of concerns.
import java.io.Serializable; 
				public class PriceData implements Serializable{
				/**This class is used to get and set open,high,low,close and volume .
					 * 
					 */
					private static final long serialVersionUID = -5027672479846021979L;
				private double open;
				private double high;
				private double low;
				private double close;
				private double volume;
				//Getters and Setters
				public double getOpen() {
					return open;
				}
				public void setOpen(double open) {
					this.open = open;
				}
				public double getHigh() {
					return high;
				}
				public void setHigh(double high) {
					this.high = high;
				}
				public double getLow() {
					return low;
				}
				public void setLow(double low) {
					this.low = low;
				}
				public double getClose() {
					return close;
				}
				public void setClose(double close) {
					this.close = close;
				}
				public double getVolume() {
					return volume;
				}
				public void setVolume(double volume) {
					this.volume = volume;
				}
				public String toString() 
				 
			    { 
			        return open + " " + high + " " +low+ " "+ close+" "+ volume; 
			    }
				public PriceData() {
					
					this.open = 0.0;
					this.high = 0.0;
					this.low = 0.0;
					this.close = 0.0;
					this.volume = 0.0;
				} 
				}
					