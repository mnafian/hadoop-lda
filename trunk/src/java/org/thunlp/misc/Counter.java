package org.thunlp.misc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;

public class Counter<KeyType> {
	
	private Hashtable<KeyType, Long> hash;
	
	public Iterator<Entry<KeyType, Long>> iterator() {
		return hash.entrySet().iterator();
	}
	
	public void clear() {
		hash.clear();
	}
	
	public int size() {
		return hash.size();
	}
	
	public Counter() {
		hash = new Hashtable<KeyType, Long>();
	}
	
	public void inc(KeyType key, long delta) {
		hash.put(key, get(key)+delta);
	}
	
	public long get(KeyType key) {
		Long current = hash.get(key);
		if ( current == null ) {
			current = 0l;
		}
		return current;
	}
}
