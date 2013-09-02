/**
 * This Class holds several utility functions collected over the time from Internet to support different functionalities
 */

package jkamal.prototype.util;

import java.util.List;

public class UtilityFunctions {
	public UtilityFunctions() {}	
	
	/**
	 * The isSet() function will take a generic type List<> and an index as arguments and will check whether this index is
	 * present within the List<> or not
	 */
	public static boolean isSet(List<?> list, int index) {
		   return index >=0 && index < list.size() && list.get(index) != null;
	}
}