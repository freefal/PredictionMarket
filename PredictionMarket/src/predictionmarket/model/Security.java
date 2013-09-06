package predictionmarket.model;

public class Security {
	public long id; // id used in the db for this security
	public String desc; // Name of security
	public long contractsize; // Type of security (future, options, etc.)
	public long begin;
	public long end;
}
