package io.ddf2.datasource.fileformat;

public class TextFile implements IFileFormat {

	protected String delimiter;
	protected String quote;
	protected boolean firstRowIsHeader;
	public TextFile(String delimiter,String quote,boolean fistRowIsHeader){
		delimiter = delimiter; quote = quote; firstRowIsHeader = fistRowIsHeader;
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
 
