package capstone.task2;

public class FlightInformation 
{
	public static enum ColumnNames 
	{ 
		Origin(0), 
		Dest(1), 
		AirlineID(2), 
		AirlineCode(3), 
		ArrDelayed(4), 
		DepDelayed(5), 
		ArrDelayMinutes(6), 
		FlightDate(7), 
		DayOfWeek(8), 
		CRSDepatureTime(9), 
		ArrTime(10),
		DepTime(11),
		FlightId(12);
		
	    private final int value;
	
	    ColumnNames(final int newValue) {
	        value = newValue;
	    }
	
	    public int getValue() { return value; }
	}
	
	private static final int COLUMNS_COUNT = 11;
	
	String[] values;
	ColumnNames[] keys;
	
	public FlightInformation(String informationRowLine, ColumnNames[] columnNames)
	{
		String[] parts = informationRowLine.split(",");
		
		if (parts.length < COLUMNS_COUNT)
		{
			String[] allColumns = new String[COLUMNS_COUNT];
		
			for (int i = 0; i < allColumns.length; i++) {
				if (i < parts.length && parts[i] != null) {
					allColumns[i] = parts[i].trim();
				} else {
					allColumns[i] = "";
				}
			}
			
			parts = allColumns;
		}
		
		values = new String[columnNames.length];
		keys = new ColumnNames[columnNames.length];

		for (int i = 0; i < columnNames.length; i++)
		{
			values[i] = parts[columnNames[i].getValue()].trim();
			
			if (values[i].equals("NA"))
				values[i] = "";
			
			keys[i] = columnNames[i];
		}
		
	}
	
	public String[] GetValues()
	{
		return values;
	}
	
	public String GetValue(ColumnNames columnName)
	{
		for (int i = 0; i < keys.length; i++)
		{
			if (keys[i] == columnName)
			{
				return values[i];
			}
		}
		
		return "";
	}
	
	public ColumnNames[] GetKeys()
	{
		return keys;
	}
}