package com.stock;

import java.io.Serializable; 
import java.util.Date;

//Here, StockPrice is a Java class to map the values of the JSON.
				public class StockPrice  implements Serializable
				{
				/**
					 * This class is used to get and set symbol,timestamp and PriceData .
					 */
					private static final long serialVersionUID = 1L;
				private PriceData priceData;
				private String symbol;
				private Date timestamp;
				//Getter and Setters
				public PriceData getPriceData() {
					return priceData;
				}
				public void setPriceData(PriceData priceData) {
					this.priceData = priceData;
				}
				public String getSymbol() {
					return symbol;
				}
				public void setSymbol(String symbol) {
					this.symbol = symbol;
				}
				public Date getTimestamp() {
					return timestamp;
				}
				public void setTimestamp(Date timestamp) {
					this.timestamp = timestamp;
				}
				
				public String toString() 
				
			    { 
			        return symbol + " " + timestamp + " " +priceData ; 
			    }  
				}