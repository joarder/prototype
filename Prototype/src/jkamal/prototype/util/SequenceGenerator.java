/**
 * @author Joarder Kamal
 */

package jkamal.prototype.util;

import java.io.Serializable;

@SuppressWarnings("serial")
public class SequenceGenerator implements Serializable {
	private int next = 0;
	
    public synchronized int getNext() { 
        return ++next; 
    }
}
