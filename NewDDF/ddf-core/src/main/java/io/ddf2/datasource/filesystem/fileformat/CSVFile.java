package io.ddf2.datasource.filesystem.fileformat;

public class CSVFile implements IFileFormat {

	public static final String TAB_SEPARATOR = "\\t";
	public static final String COMMA_SEPARATOR = ",";

	protected String delimiter;
	protected String quote;
	protected boolean firstRowIsHeader;
	public CSVFile(String delimiter){
		this(delimiter,"",false);
	}
	public CSVFile(String delimiter, String quote, boolean fistRowIsHeader){
		this.delimiter = delimiter; this.quote = quote; this.firstRowIsHeader = fistRowIsHeader;
	}
	public String getDelimiter() {
		return delimiter;
	}
	 
	public String getQuote() {
		return quote;
	}

	public boolean firstRowIsHeader() {
		return firstRowIsHeader;
	}
}
 
