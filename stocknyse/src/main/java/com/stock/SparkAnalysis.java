package com.stock;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


import org.apache.log4j.*;

import scala.Tuple2;

public class SparkAnalysis implements Serializable{
	

	private static final long serialVersionUID = 1L;
	

	public static void main(String[] args) throws InterruptedException {
		
				System.setProperty("hadoop.home.dir","C:\\hadoop" );//to set system properties for winutils.exe
				SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStock");
				//Batch interval set to 1min
				JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
				jssc.checkpoint("checkpoint_dir");//checkpoint directory is assigned
				Logger.getRootLogger().setLevel(Level.ERROR);
				
				//to read json files at regular batch intervals.
				JavaDStream<String> json=jssc.textFileStream(args[0]);
				System.out.println("json");
				json.print();
				
				// Convert json into DStream
				JavaDStream<Map<String, StockPrice>> stockStream =
						 
						 json.map(x -> {
						ObjectMapper mapper = new ObjectMapper();
						SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						mapper.setDateFormat(df);
						
						TypeReference<List<StockPrice>> mapType = new
						TypeReference<List<StockPrice>>() {
						};
						List<StockPrice> list = mapper.readValue(x, mapType);
						Map<String, StockPrice> map = new HashMap<>();
						for (StockPrice sp : list) {
							map.put(sp.getSymbol(),sp);
						}
						return map;
						});
				 System.out.println("stockStream");		
				
				
				/*
				 * Coding to perform analysis-1&2.It gives output in ((avgopen,avgclose,maxprofit,count),symbol) which is sorted by maxprofit.
				 */
				 
				//To Get average closing price and maxprofit data related to the four stocks in the window from the above DStream.
					
				JavaPairDStream<String, AverageTuple> windowAvgDStream = getAvgWindowDStream(stockStream);
				//key and value are swapped to help sort by maxprofit.
				JavaPairDStream<AverageTuple, String> swappedwindowAvgDStream = windowAvgDStream.mapToPair(x -> x.swap());
				
				//sorting by maxprofit field of AverageTuple
				JavaPairDStream<AverageTuple, String> profitSorted = swappedwindowAvgDStream.transformToPair(
			       	     new Function<JavaPairRDD<AverageTuple, String>, JavaPairRDD<AverageTuple, String>>() {
			       	        
							private static final long serialVersionUID = 1L;

							public JavaPairRDD<AverageTuple, String> call(JavaPairRDD<AverageTuple, String> in) throws Exception {
						
			       	           return in.sortByKey(new ProfitComparator());
			       	         }
			       	       });
				profitSorted.print();	
				
				//To generate output files of Average closing price and maxprofit
				profitSorted.foreachRDD(new VoidFunction<JavaPairRDD<AverageTuple,String>>() {
					
					private static final long serialVersionUID = 6767679;
					public void call(JavaPairRDD< AverageTuple,String> t)
					throws Exception {
					t.coalesce(1).saveAsTextFile(args[1]+"\\MaxAvg"+java.io.File.separator + System.currentTimeMillis());
					}
					});
				
				/*
				 * Coding to perform analysis-3(RSI),which gives output in format (symbol,rsi).	
				 */
				
				//To Get RSI data related to the four stocks in the window from the above DStream .
				JavaPairDStream<String, RSITuple> windowRSIDStream = getRSIWindowDStream(stockStream);
				windowRSIDStream.print();	
				
				//To generate output files
				windowRSIDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, RSITuple>>() {
					/**
					*
					*/
					private static final long serialVersionUID = 6767679;
					public void call(JavaPairRDD< String, RSITuple> t)
					throws Exception {
					t.coalesce(1).saveAsTextFile(args[1]+"\\RSI"+java.io.File.separator + System.currentTimeMillis());
					}
					});
				
				
				/*
				 * Coding to perform analysis-4(Volume),which gives output in format (volume,symbol) sorted by volume.	
				 */	
				
				//To Get Volume data related to the four stocks in the window from the above DStream.
				JavaPairDStream<String, Double> windowVolumeDStream =getVolumeWindowDStream(stockStream);
				
				//to swap key and value in (volume,symbol) format.
				JavaPairDStream<Double, String> swappedVolumeDStream = windowVolumeDStream.mapToPair(x -> x.swap());
				//to sort by key(volume).
				JavaPairDStream<Double, String> volsorted = swappedVolumeDStream.transformToPair(
			       	     new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
			       	         /**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							public JavaPairRDD<Double, String> call(JavaPairRDD<Double, String> in) throws Exception {
			       	           return in.sortByKey(false);
			       	         }
			       	       });
				
				
				volsorted.print();
				
				//To generate output files
				volsorted.foreachRDD(new VoidFunction<JavaPairRDD<Double, String>>() {
					/**
					*
					*/
					private static final long serialVersionUID = 6767679;
					public void call(JavaPairRDD<Double, String> t)
					throws Exception {
					t.coalesce(1).saveAsTextFile(args[1]+"\\Volume"+java.io.File.separator + System.currentTimeMillis());
					}
					});
				
				
	jssc.start();
	jssc.awaitTermination();

}
//////////Analysis -1&2 calling functions and summary and inverse function////////////////
	/*Here, SUM_REDUCER_AVG_DATA and DIFF_REDUCER_AVG_DATA
	are Lambda Expressions.*/
	private static Function2<AverageTuple, AverageTuple, AverageTuple> SUM_REDUCER_AVG_DATA = (a, b) -> {
		AverageTuple avg = new AverageTuple();
		avg.setCount(a.getCount() + b.getCount());
		avg.setAverageClose(a.getAverageClose() + b.getAverageClose());
		avg.setAverageOpen(a.getAverageOpen()+ b.getAverageOpen());
		avg.setMaxprofit((a.getAverageClose() + b.getAverageClose()),(a.getAverageOpen()+ b.getAverageOpen()), (a.getCount() + b.getCount()));
		return avg;
	};
	private static Function2<AverageTuple, AverageTuple, AverageTuple> DIFF_REDUCER_AVG_DATA = (a, b) -> {
		AverageTuple avg = new AverageTuple();
		avg.setCount(a.getCount() - b.getCount());
		avg.setAverageClose(a.getAverageClose() - b.getAverageClose());
		avg.setAverageOpen(a.getAverageOpen()- b.getAverageOpen());
		avg.setMaxprofit((a.getAverageClose() - b.getAverageClose()),(a.getAverageOpen()- b.getAverageOpen()), (a.getCount() - b.getCount()));
		return avg;
	};
	
	//This function returns union of all symbols data in <String, AverageTuple> format. 
	private static JavaPairDStream<String, AverageTuple> getAvgWindowDStream(
			JavaDStream<Map<String, StockPrice>> stockStream) {
		 
		JavaPairDStream<String, AverageTuple> stockPriceStream =
									getAvgDStream(stockStream, "MSFT");
		JavaPairDStream<String, AverageTuple> stockPriceGoogleStream=
									getAvgDStream(stockStream, "GOOGL");
		JavaPairDStream<String, AverageTuple> stockPriceADBEStream =
									getAvgDStream(stockStream, "ADBE");
		JavaPairDStream<String, AverageTuple> stockPriceFBStream =
									getAvgDStream(stockStream, "FB");
				
		//reduce by key(symbol) and window of 10 mins and sliding window 5 mins		
		JavaPairDStream<String, AverageTuple> windowMSFTDStream =
						stockPriceStream.reduceByKeyAndWindow(SUM_REDUCER_AVG_DATA,DIFF_REDUCER_AVG_DATA, 
						Durations.minutes(10),Durations.minutes(5));
		
		JavaPairDStream<String, AverageTuple> windowGoogDStream =
						stockPriceGoogleStream.reduceByKeyAndWindow(SUM_REDUCER_AVG_DATA,DIFF_REDUCER_AVG_DATA, 
						Durations.minutes(10),Durations.minutes(5));
				
		JavaPairDStream<String, AverageTuple> windowAdbDStream =
						stockPriceADBEStream.reduceByKeyAndWindow(SUM_REDUCER_AVG_DATA,DIFF_REDUCER_AVG_DATA, 
						Durations.minutes(10),Durations.minutes(5));
				
		JavaPairDStream<String, AverageTuple> windowFBDStream =
						stockPriceFBStream.reduceByKeyAndWindow(SUM_REDUCER_AVG_DATA,DIFF_REDUCER_AVG_DATA,
						 Durations.minutes(10),Durations.minutes(5));
		//union of all symbol streams		
		windowMSFTDStream =windowMSFTDStream.union(windowGoogDStream).union(windowAdbDStream).union(windowFBDStream);
				return windowMSFTDStream;
	}
	
	//This function is used to get Average tuple fields from stockPrice object for particular symbol.
	private static JavaPairDStream<String, AverageTuple> getAvgDStream(JavaDStream<Map<String, StockPrice>> stockStream,
			String symbol) {
		
		JavaPairDStream<String, AverageTuple> stockAvgStream = stockStream
				.mapToPair(new PairFunction<Map<String, StockPrice>,String, AverageTuple>() {
				
		private static final long serialVersionUID = 1L;

				public Tuple2<String, AverageTuple>
				call(Map<String, StockPrice> map) throws Exception {
				if (map.containsKey(symbol)) {
					
					//(symbol,AverageTuple(count,  averageOpen, averageClose,maxprofit))
					return new Tuple2<String,AverageTuple>(symbol, 
						new AverageTuple(1, map.get(symbol).getPriceData().getOpen(),
								map.get(symbol).getPriceData().getClose(),(map.get(symbol).getPriceData().getClose()-map.get(symbol).getPriceData().getOpen())
								));
				} else {
					
				return new Tuple2<String,AverageTuple>(symbol, new AverageTuple());
				}
				}
				});
				return stockAvgStream;
				}
	
///////////Analysis -3 RSI functions and summary and inverse function/////////////////////
	/*Here, SUM_REDUCER_PRICE_DATA and DIFF_REDUCER_PRICE_DATA
	are Lambda Expressions.*/
	private static Function2<RSITuple, RSITuple, RSITuple> SUM_REDUCER_RSI_DATA = (a, b) -> {
		RSITuple rs = new RSITuple();
		double gain,loss;
		//current close- previous close is calculated and assigned to gain and loss =0 if positive ,else vice versa.
		if((b.getClose() - a.getClose())>=0) {
			gain=b.getClose() - a.getClose();
			loss=0;
		}else{
			loss=Math.abs(b.getClose() - a.getClose());
			gain=0;
		}
		
		//current close is swapped with previous close
		rs.setClose(b.getClose());
		
		//count is incremented
		int count=a.getCount()+b.getCount();
		rs.setCount(a.getCount()+b.getCount());
		
		//Adding all gain and loss for first 9 elements and saving it in avgGain,avgLoss fields of the RSITuple respectively
		if(count<10) {
			
			rs.setAvgGain(a.getAvgGain()+gain);
			rs.setAvgLoss(a.getAvgLoss()+loss);
					
		}
		//When count is 10 
		else if(count==10) {
			//System.out.println("prevavggain:"+a.getAvgGain()+"prevavgloss:"+a.getAvgLoss()+"gain:"+gain+"loss:"+loss);
			rs.setAvgGain((a.getAvgGain()+gain)/10);
			rs.setAvgLoss((a.getAvgLoss()+loss)/10);
			
		}else {
			
			//System.out.println("prevavggain:"+a.getAvgGain()+"prevavgloss:"+a.getAvgLoss()+"gain:"+gain+"loss:"+loss);
			rs.setAvgGain(((a.getAvgGain()*9)+gain)/10);
			rs.setAvgLoss(((a.getAvgLoss()*9)+loss)/10);
			
		}
		return rs;};
		
		//At inverse function earlier tuple(a) is assigned to the new one(b) to maintain the state of tuple fields.
	private static Function2<RSITuple, RSITuple, RSITuple> DIFF_REDUCER_RSI_DATA = (a, b) -> {
		RSITuple rs = new RSITuple();
		rs.setCount(a.getCount());
		rs.setClose(a.getClose());
		rs.setAvgGain(a.getAvgGain());
		rs.setAvgLoss(a.getAvgLoss());
		
		return rs;};
	
		
	//this function returns union of all symbols in format <String, RSITuple>.
	private static JavaPairDStream<String, RSITuple> getRSIWindowDStream(
			
			JavaDStream<Map<String, StockPrice>> stockStream) {
		// TODO Auto-generated method stub
		JavaPairDStream<String, RSITuple> stockPriceStream =
				getRSIDStream(stockStream, "MSFT");
				//stockPriceStream.print();
				JavaPairDStream<String, RSITuple> stockPriceGoogleStream
				= getRSIDStream(stockStream, "GOOGL");
				JavaPairDStream<String, RSITuple> stockPriceADBEStream =
						getRSIDStream(stockStream, "ADBE");
				JavaPairDStream<String, RSITuple> stockPriceFBStream =
						getRSIDStream(stockStream, "FB");
				
				//Streams are generated at window =10 min and sliding = 1 min.
				JavaPairDStream<String, RSITuple> windowMSFTDStream =
				stockPriceStream.reduceByKeyAndWindow(SUM_REDUCER_RSI_DATA,DIFF_REDUCER_RSI_DATA,
				 Durations.minutes(10),
				Durations.minutes(1));
				JavaPairDStream<String, RSITuple> windowGoogDStream =
				stockPriceGoogleStream.reduceByKeyAndWindow(SUM_REDUCER_RSI_DATA, DIFF_REDUCER_RSI_DATA,
						 Durations.minutes(10),
				Durations.minutes(1));
				JavaPairDStream<String, RSITuple> windowAdbDStream =
				stockPriceADBEStream.reduceByKeyAndWindow(SUM_REDUCER_RSI_DATA, DIFF_REDUCER_RSI_DATA,
						 Durations.minutes(10),
				Durations.minutes(1));
				JavaPairDStream<String, RSITuple> windowFBDStream =
				stockPriceFBStream.reduceByKeyAndWindow(SUM_REDUCER_RSI_DATA, DIFF_REDUCER_RSI_DATA,
						 Durations.minutes(10),
				Durations.minutes(1));
				//Union of all symbol streams 
				windowMSFTDStream =windowMSFTDStream.union(windowGoogDStream).union(windowAdbDStream).union(windowFBDStream);
				return windowMSFTDStream;
	}
	
	

//getRSIDStream function is used get RSITuple values for each stock from StockPrice class object.
private static JavaPairDStream<String, RSITuple> getRSIDStream(JavaDStream<Map<String, StockPrice>> stockStream,
			String symbol) {
		// TODO Auto-generated method stub
		JavaPairDStream<String, RSITuple> stockRSIStream = stockStream
				.mapToPair(new PairFunction<Map<String, StockPrice>,String, RSITuple>() {
				/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

				public Tuple2<String, RSITuple>
				call(Map<String, StockPrice> map) throws Exception {
				if (map.containsKey(symbol)) {
					
					//RSITuple(close,avgGain,avgLoss,count) in this closing price and count are assigned from StockPrice Class object 
					
					return new Tuple2<String,RSITuple>(symbol, new RSITuple(map.get(symbol).getPriceData().getClose(), 0, 0, 1));
					
				} else {
					//if symbol doesnt exist an empty tuple is created.
					
				return new Tuple2<String,RSITuple>(symbol, new RSITuple());
				}
				}
				});
				
				return stockRSIStream;
				}
	

///////////Analysis-4 Volume calling functions and Summary and inverse functions.//////////////////////
/*Here, SUM_REDUCER_VOLUME_DATA and DIFF_REDUCER_VOLUME_DATA
are Lambda Expressions.*/
private static Function2<Double, Double, Double>
SUM_REDUCER_VOLUME_DATA = (a, b) -> {
return a+b;};
private static Function2<Double, Double, Double>
DIFF_REDUCER_VOLUME_DATA = (a, b) -> {
	return a-b;
};

//this function is used to get volume from stockPrice object for all symbols.
private static JavaPairDStream<String, Double> getVolumeWindowDStream(
		JavaDStream<Map<String, StockPrice>> stockStream) {
	JavaPairDStream<String, Double> stockPriceStream =
			getVolumeDStream(stockStream, "MSFT");
	JavaPairDStream<String, Double> stockPriceGoogleStream
			= getVolumeDStream(stockStream, "GOOGL");
	JavaPairDStream<String, Double> stockPriceADBEStream =
			getVolumeDStream(stockStream, "ADBE");
	JavaPairDStream<String, Double> stockPriceFBStream =
			getVolumeDStream(stockStream, "FB");
	
	
	JavaPairDStream<String, Double> windowMSFTDStream =
			stockPriceStream.reduceByKeyAndWindow(SUM_REDUCER_VOLUME_DATA, DIFF_REDUCER_VOLUME_DATA, 
					Durations.minutes(10), Durations.minutes(10));
	JavaPairDStream<String, Double> windowGoogDStream =
			stockPriceGoogleStream.reduceByKeyAndWindow(SUM_REDUCER_VOLUME_DATA,DIFF_REDUCER_VOLUME_DATA,
			Durations.minutes(10),Durations.minutes(10));
	JavaPairDStream<String, Double> windowAdbDStream =
			stockPriceADBEStream.reduceByKeyAndWindow(SUM_REDUCER_VOLUME_DATA,DIFF_REDUCER_VOLUME_DATA,
			 Durations.minutes(10),Durations.minutes(10));
	JavaPairDStream<String, Double> windowFBDStream =
			stockPriceFBStream.reduceByKeyAndWindow(SUM_REDUCER_VOLUME_DATA,DIFF_REDUCER_VOLUME_DATA, 
			 Durations.minutes(10),Durations.minutes(10));
	windowMSFTDStream = windowMSFTDStream.union(windowGoogDStream).union(windowAdbDStream).union(windowFBDStream);
		 return windowMSFTDStream; 
		 
}

//Here, getPriceDStream is used to get volume from stockPrice object for particular symbol.
private static JavaPairDStream<String, Double> getVolumeDStream(JavaDStream<Map<String, StockPrice>> stockStream,String symbol) {
JavaPairDStream<String, Double> stockVolumeStream = stockStream
.mapToPair(new PairFunction<Map<String, StockPrice>,
String, Double>() {

	private static final long serialVersionUID = 1L;

public Tuple2<String, Double>
call(Map<String, StockPrice> map) throws Exception {
if (map.containsKey(symbol)) {
return new Tuple2<String,
		Double>(symbol, map.get(symbol).getPriceData().getVolume());
} else {
return new Tuple2<String,
		Double>(symbol, 0.0);
}
}
});
return stockVolumeStream;
}


}


