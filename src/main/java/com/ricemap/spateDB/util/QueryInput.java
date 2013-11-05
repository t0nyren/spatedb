package com.ricemap.spateDB.util;

public class QueryInput {
	public enum QUERY_TYPE {Get, Distinct, Distribution, Average};
	public QUERY_TYPE type;
	public String field;
	

	public QueryInput() {
		type = QUERY_TYPE.Get;
	}

}
