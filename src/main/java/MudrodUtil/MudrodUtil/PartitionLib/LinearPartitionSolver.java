package MudrodUtil.MudrodUtil.PartitionLib;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LinearPartitionSolver implements ThePartitionProblemSolver {

	public int[] solve(double[] nums, int k) {
		assert k > 0 && nums.length >= k;

		// prefix sums: sum[k] = books[i..k]
		final double[] sum = new double[nums.length];

		sum[0] = nums[0];
		for (int i = 1; i < nums.length; i++)
			sum[i] = sum[i - 1] + nums[i];

		// M[n<=length][m<=k], D[n<=length][m<=k]
		final double[][] M = new double[nums.length + 1][k + 1];
		final double[][] D = new double[nums.length + 1][k + 1];

		for (int n = 1; n <= nums.length; n++)
			M[n][1] = sum[n - 1];
		for (int m = 1; m <= k; m++)
			M[1][m] = nums[0];

		for (int n = 2; n <= nums.length; n++) {
			for (int m = 2; m <= k; m++) {
				M[n][m] = Integer.MAX_VALUE;
				for (int x = 1; x < n; x++) {
					final double largest = Math.max(M[x][m - 1], sum[n - 1] - sum[x - 1]);

					if (largest < M[n][m]) {
						M[n][m] = largest;
						D[n][m] = x;
					}
				}
			}
		}

		int[] dividers = new int[k - 1];
		for (int m = k, n = nums.length; m > 1; m--)
			n = dividers[m - 2] = (int) D[n][m];
		return dividers;
	}

	public Map<String, Integer> solve(Map<String, Double> labelNums, int k) {

		List<Double> nums = new ArrayList(labelNums.values());
		List<String> labels = new ArrayList(labelNums.keySet());
		
		double[] numArr = new double[nums.size()];
		for(int i=0; i<nums.size(); i++){
			numArr[i] = nums.get(i);
		}
		int[] dividers = this.solve(numArr, k);
		System.out.println(labelNums);
		for (int i = 0; i < dividers.length; i++) {
			System.out.println(dividers[i]);
		}
		
		int labelNum = labels.size();
		int group = 0;
		Map<String, Integer> labelGroups = new HashMap<String, Integer>();
		for(int i=0; i<labelNum; i++){
			
			int divider = dividers[group];
			if(i<=divider){
				labelGroups.put(labels.get(i), group);
			}else{
				group += 1;
				labelGroups.put(labels.get(i), group);
			}
		}
		
		return labelGroups;
	}

	public static void main(String[] args) throws ParseException, IOException, InterruptedException {
		// TODO Auto-generated method stub

		ThePartitionProblemSolver solution = new LinearPartitionSolver();

		double[] books = { 541, 745, 751, 830, 943, 977, 983, 1020, 1087, 1110, 1287, 1387 };
		int[] dividers = solution.solve(books, 4);
		for (int i = 0; i < dividers.length; i++) {
			System.out.println(dividers[i]);
		}
	}
}
