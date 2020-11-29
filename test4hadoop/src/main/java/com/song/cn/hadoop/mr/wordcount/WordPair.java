package com.song.cn.hadoop.mr.wordcount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class WordPair implements WritableComparable<WordPair> {
	private String wordA;
	private String wordB;

	public WordPair() {
	}

	public WordPair(String wordA, String wordB) {
		this.wordA = wordA;
		this.wordB = wordB;
	}

	public String getWordA() {
		return this.wordA;
	}

	public String getWordB() {
		return this.wordB;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(wordA);
		out.writeUTF(wordB);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		wordA = in.readUTF();
		wordB = in.readUTF();
	}

	@Override
	public String toString() {
		return wordA + "," + wordB;
	}

	@Override
	public int compareTo(WordPair o) {
		if (this.equals(o))
			return 0;
		else
			return (wordA + wordB).compareTo(o.getWordA() + o.getWordB());
	}

	@Override
	public boolean equals(Object o) {
		//无序对，不用考虑顺序
		if (!(o instanceof WordPair))
			return false;
		WordPair w = (WordPair) o;
		if ((this.wordA.equals(w.wordA) && this.wordB.equals(w.wordB))
				|| (this.wordB.equals(w.wordA) && this.wordA.equals(w.wordB)))
			return true;
		return false;
	}

	@Override
	public int hashCode() {
		return (wordA.hashCode() + wordB.hashCode()) * 17;
	}

}
