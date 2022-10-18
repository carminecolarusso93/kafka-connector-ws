package database;

public class DatabaseNotConnectException  extends Exception {
    private static final long serialVersionUID = 1L;

	public DatabaseNotConnectException(String msg) {
		super(msg);
	}
}
