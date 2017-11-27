package com.sc3.topics;

import java.io.Serializable;

/**
 * @author stephen.carton@gmail.com
 * @date Nov 26, 2017
 *
 */
public class TextStuff implements Serializable{
	private static final long serialVersionUID = 6077020446573046211L;
	private String id;
	private String[] tokens;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String[] getTokens() {
		return tokens;
	}
	public void setTokens(String[] tokens) {
		this.tokens = tokens;
	}
	public static TextStuff apply(String id, String[] tks) {
		TextStuff ts = new TextStuff();
		ts.setTokens(tks);
		ts.setId(id);
		return ts;
	}
}
