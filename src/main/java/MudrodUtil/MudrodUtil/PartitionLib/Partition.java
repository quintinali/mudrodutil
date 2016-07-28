/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package MudrodUtil.MudrodUtil.PartitionLib;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author samuel
 */
public class Partition {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

    	String filepath = "E:/mudrod/podacc logs";
    	Map<String, Double> monthsize = extractFileSize(filepath);
    	
    	//System.out.println(monthsize);
    	
    	particion(monthsize, 4);
       /* List<Double> lista = new ArrayList();
        lista.add(1387.0);
        lista.add(1287.0);
        lista.add(541.0);
        lista.add(1100.0);
        lista.add(1020.0);
        
        lista.add(943.0);
        lista.add(983.0);
        lista.add(830.0);
        lista.add(745.0);
        lista.add(751.0);
        
        lista.add(977.0);
        lista.add(1087.0);*/
        
       /* lista.add(18776555.0);
        lista.add(20399410.0);
        lista.add(7366199.0);
        lista.add(13741258.0);
        lista.add(16776275.0);
        
        lista.add(15631101.0);
        lista.add(14398782.0);
        lista.add(10205596.0);
        lista.add(9232653.0);
        lista.add(10202290.0);
        
        lista.add(10344742.0);
        lista.add(7894828.0);*/
        
        //particion(lista, 4);
    }
    
    public static  List<List<String>> particion(Map<String, Double> monthsize, int k) {
    	
    	System.out.println(monthsize);
    	
    	LinkedHashMap sortedMap = sortMapByValue(monthsize);
    	
    	List<Double> lista = new ArrayList(sortedMap.values());
    	List<String> months = new ArrayList(sortedMap.keySet());
    	
    	System.out.println(lista);
    	System.out.println(months);
    	
    	//Collections.sort(lista, Collections.reverseOrder());
    	
    	System.out.println(lista);
    	System.out.println(months);
    	
    	List<List<Double>> parts = new ArrayList<List<Double>>();
    	List<List<String>> splitMonths = new ArrayList<List<String>>();
        
        for(int i=0; i<k; i++){
        	 List<Double> part = new ArrayList();
        	 parts.add(part);
        	 
        	 List<String> monthList = new ArrayList();
        	 splitMonths.add(monthList);
        }

        int j =0;
        for (Double lista1 : lista) {
        	
        	Double minimalSum = -1.0;
        	int position = 0;
        	for(int i=0; i<parts.size(); i++){
        		List<Double> part  = parts.get(i);
        		if(minimalSum == -1){
        			minimalSum = Suma(part);
        			position = i;
        		}
        		else if(Suma(part) < minimalSum ){
        			minimalSum = Suma(part);
        			position = i;
        		}
        	}
        	
        	List<Double> part  = parts.get(position);
        	part.add(lista1);
        	parts.set(position, part);
        	
        	List<String> month  = splitMonths.get(position);
        	month.add(months.get(j));
        	splitMonths.set(position, month);
        	j++;
        }
        
        for(int i=0; i<parts.size(); i++){
        	print(parts.get(i));
        }
        
        for(int i=0; i<splitMonths.size(); i++){
        	printStrList(splitMonths.get(i));
        }
        
        return splitMonths;
    }

    static void particion(List<Double> lista, int k) {
    	
    	Collections.sort(lista, Collections.reverseOrder());
    	
    	List<List<Double>> parts = new ArrayList<List<Double>>();
        
        for(int i=0; i<k; i++){
        	 List<Double> part = new ArrayList();
        	 parts.add(part);
        }

        for (Double lista1 : lista) {
        	
        	Double minimalSum = -1.0;
        	int position = 0;
        	for(int i=0; i<parts.size(); i++){
        		List<Double> part  = parts.get(i);
        		if(minimalSum == -1){
        			minimalSum = Suma(part);
        			position = i;
        		}
        		else if(Suma(part) < minimalSum ){
        			minimalSum = Suma(part);
        			position = i;
        		}
        	}
        	
        	List<Double> part  = parts.get(position);
        	part.add(lista1);
        	parts.set(position, part);
        }
        
        for(int i=0; i<parts.size(); i++){
        	print(parts.get(i));
        }
        
    }

    static Double Suma(List<Double> part) {
    	Double ret = 0.0;
        for (int i = 0; i < part.size(); i++) {
            ret += part.get(i);
        }
        return ret;
    }

    static void print(List<Double> list){
        for (int i = 0; i < list.size(); i++) {
            System.out.print(list.get(i)+",");
        }
        System.out.print("sum is:" + Suma(list));
        System.out.println();
    }
    
    static void printStrList(List<String> list){
        for (int i = 0; i < list.size(); i++) {
            System.out.print(list.get(i)+",");
        }
        System.out.println();
    }
    
    public static LinkedHashMap<String, Double> sortMapByValue(Map passedMap) {
		List mapKeys = new ArrayList(passedMap.keySet());
		List mapValues = new ArrayList(passedMap.values());
		Collections.sort(mapValues, Collections.reverseOrder());
		Collections.sort(mapKeys,Collections.reverseOrder());

		LinkedHashMap sortedMap = new LinkedHashMap();

		Iterator valueIt = mapValues.iterator();
		while (valueIt.hasNext()) {
			Object val = valueIt.next();
			Iterator keyIt = mapKeys.iterator();

			while (keyIt.hasNext()) {
				Object key = keyIt.next();
				String comp1 = passedMap.get(key).toString();
				String comp2 = val.toString();

				if (comp1.equals(comp2)){
					passedMap.remove(key);
					mapKeys.remove(key);
					sortedMap.put((String)key, (Double)val);
					break;
				}

			}

		}
		return sortedMap;
	}
    
	public static Map<String, Double> extractFileSize(String filepath) {

		File file = new File(filepath);
		File[] filelist = file.listFiles();
		
		Map<String, Double> monthsize = new HashMap<String, Double>();
		DecimalFormat df = new DecimalFormat("#.##");
		
		for (int i = 0; i < filelist.length; i++) {
			if (filelist[i].isFile()) {
				String filename = filelist[i].getPath();
				String time_suffix = filename.substring(Math.max(filename.length() - 6, 0));
				double fileS = filelist[i].length()/1073741824.0;
				double curSize = 0.0;
				if(monthsize.containsKey(time_suffix)){
					curSize = monthsize.get(time_suffix);
				}
				monthsize.put(time_suffix, Double.parseDouble(df.format(curSize + fileS)));
			}
		}
		
		return monthsize;
	}
}