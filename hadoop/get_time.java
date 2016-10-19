import org.apache.hadoop.mapreduce.TaskReport;
public class get_time
{
	public static void main(String[] args)
	
	{
		TaskReport[] maps = jobtracker.getMapTaskReports("your_job_id");
		for (TaskReport rpt : maps) {
		  long duration = rpt.getFinishTime() - rpt.getStartTime();
		  System.out.println("Mapper duration: " + duration);
		}
		TaskReport[] reduces = jobtracker.getReduceTaskReports("your_job_id");
		for (TaskReport rpt : reduces) {
		  long duration = rpt.getFinishTime() - rpt.getStartTime();
		  System.out.println("Reducer duration: " + duration);
		}
	}
}