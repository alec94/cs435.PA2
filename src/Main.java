import util.AuthorDetectDriver;
import util.OfflineDriver;

/**
 * Created by Alec on 3/24/2017.
 * Main program driver
 */
public class Main {
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("You forgot the mode.");
			System.exit(-1);
		}

		if (args[2].equals("offline")) {
			try {
				OfflineDriver.main(args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			try {
				AuthorDetectDriver.main(args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
