package MudrodUtil.MudrodUtil.PartitionLib;

import java.util.Map;

public interface ThePartitionProblemSolver {

	public int[] solve(double[] books, int k);
	public Map<String, Integer> solve(Map<String, Double> labelNums, int k);
}
