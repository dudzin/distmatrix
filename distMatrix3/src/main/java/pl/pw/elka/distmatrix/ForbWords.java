package pl.pw.elka.distmatrix;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ForbWords {

	private ArrayList<String> words;
	
	public ForbWords() {
		words = new ArrayList<String>();
	}
	
	
	public boolean isWorb(String word){
		word = word.trim();
		for (String w : words) {
			if(w.equals(word)) return true;
		}
		return false;
	}
	

	public ArrayList<String> getWords() {
		return words;
	}

	public void setWords(ArrayList<String> words) {
		this.words = words;
	}
	
	public void addWord(String word){	
		words.add(word.trim());
	}
}
