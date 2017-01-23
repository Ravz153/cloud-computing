public class Timer
{
	long startTime, endTime, elapsedTime;

	Timer()
	{
		startTime = System.currentTimeMillis();
	}

	public void start()
	{
		startTime = System.currentTimeMillis();
	}

	public Timer end()
	{
		endTime = System.currentTimeMillis();
		elapsedTime = endTime-startTime;
		return this;
	}

	public String toString()
	{
		return "Time: " + elapsedTime + " msec.";
	}
}