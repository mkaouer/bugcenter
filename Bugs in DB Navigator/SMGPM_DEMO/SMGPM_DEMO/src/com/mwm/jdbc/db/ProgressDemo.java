package com.mwm.jdbc.db;

public class ProgressDemo {
	  static void updateProgress(double progressPercentage) 
	  {
	    final int width = 50; // progress bar width in chars

	    System.out.print("\r[");
	    int i = 0;
	    for (; i <= (int)(progressPercentage*width); i++) {
	      System.out.print(".");
	    }
	    for (; i < width; i++) {
	      System.out.print(" ");
	    }
	    System.out.print("]");
	  }

	  public static void main(String[] args) {
	    try {
	      for (double progressPercentage = 0.0; progressPercentage < 1.0; progressPercentage += 0.01) {
	        updateProgress(progressPercentage);
	        Thread.sleep(20);
	      }
	    } catch (InterruptedException e) {}
	  }
	  
	  public static void printProgBar(int percent){
		    StringBuilder bar = new StringBuilder("[");

		    for(int i = 0; i < 50; i++){
		        if( i < (percent/2)){
		            bar.append("=");
		        }else if( i == (percent/2)){
		            bar.append(">");
		        }else{
		            bar.append(" ");
		        }
		    }

		    bar.append("]   " + percent + "%     ");
		    System.out.print("\r" + bar.toString());
		}
	  
	}
