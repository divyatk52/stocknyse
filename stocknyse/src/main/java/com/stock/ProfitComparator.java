package com.stock;
import java.io.Serializable;
import java.util.Comparator;

public class ProfitComparator implements Comparator<AverageTuple>, Serializable {

	/**
	 * this class is used to compare the MaxProfit attribute of AverageTuple class
	 */
	private static final long serialVersionUID = 1L;

	public int compare(AverageTuple o1, AverageTuple o2) {
		// TODO Auto-generated method stub
		//System.out.println(o1.getMaxprofit());
		//System.out.println(o2.getMaxprofit());
		//System.out.println("diff:"+((o2.getMaxprofit()*1000000)- (o1.getMaxprofit()*1000000)));
		return (int) ((o2.getMaxprofit()*1000000)- (o1.getMaxprofit()*1000000));
		//return o2.getMaxprofit()o1.getMaxprofit();
	}

}
